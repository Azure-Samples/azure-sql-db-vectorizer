using System;
using Azure;
using Azure.AI.OpenAI;
using DotNetEnv;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using ShellProgressBar;
using Microsoft.SemanticKernel.Text;
using System.Net.Http;
using System.Data;
using System.Security.Cryptography;
using System.Threading.Tasks.Dataflow;

#pragma warning disable SKEXP0050

namespace Azure.SQL.DB.Vectorizer;

public class Program
{
    private static readonly int _queueBatchSize = 5000;
    private static readonly ConcurrentQueue<EmbeddingData> _queue = new();

    private static readonly List<Task> tasks = [];
    private static int _maxTasks = 0; // 0 to auto-detect

    private static readonly List<OpenAIClient> _openAIClients = [];
    private static readonly int _openaiBatchSize = 50;
    private static string _embeddingModel = "text-embedding-3-small";

    private static IVectorizer? _vectorizer; 
    private record ChunkedText(int RowId, int ChunkId, string Text);

    static void Main(string[] args)
    {
        if (Environment.UserInteractive && !System.Diagnostics.Debugger.IsAttached)
            Console.Clear();

        Console.WriteLine("Starting...");

        if (args.Length > 0)
        {
            Console.WriteLine($"Using {args[0]} environment file if available.");
            Env.Load(args[0]);
        } else {
            Console.WriteLine($"Using .env environment file if available.");
            Env.Load();
        }

        bool useDedicatedTable = !string.IsNullOrEmpty(Env.GetString("DEDICATED_EMBEDDINGS_TABLE"));        

        if (useDedicatedTable)
            _vectorizer = new DedicatedTableVectorizer();
        else
        {
            //_vectorizer = new SameTableVectorizer();
            throw new NotImplementedException("SameTableVectorizer not implemented yet.");
        }

        _embeddingModel = Env.GetString("OPENAI_EMBEDDING_DEPLOYMENT_NAME");

        string oaiUrls = Env.GetString("OPENAI_URL");
        string oaiKeys = Env.GetString("OPENAI_KEY");

        string[] _oaiEndpoint = oaiUrls.Split(",");
        string[] _oaiKey = oaiKeys.Split(",");

        if (_oaiEndpoint.Length != _oaiKey.Length)
        {
            throw new ApplicationException("OpenAI URL and Key count mismatch.");
        }

        foreach (var (url, key) in _oaiEndpoint.Zip(_oaiKey))
        {
            AzureKeyCredential credentials = new(key);
            OpenAIClient openAIClient = new(new Uri(url), credentials);
            _openAIClients.Add(openAIClient);
        }

        _maxTasks = _maxTasks == 0 ? _openAIClients.Count * 2 : _maxTasks;
        
        Console.WriteLine($"OpenAI Clients: {_openAIClients.Count}, Max Tasks: {_maxTasks}, Max Queue Size: {_queueBatchSize}, REST API Batch Size: {_openaiBatchSize}");
        Console.WriteLine($"Using {_vectorizer.GetType()} vectorizer...");

        Console.WriteLine("Connecting to database...");
        _vectorizer.TestConnection();

        Console.WriteLine("Initializing database...");
        _vectorizer.InitializeDatabase();

        Console.WriteLine("Processing rows...");
        Process();

        Console.WriteLine("Done.");
    }

    private static void Process()
    {
        System.Diagnostics.Debug.Assert(_vectorizer != null); 
        Exception? exception = null;

        Console.WriteLine("Getting rows count...");             
        var t = _vectorizer.GetDataCount();

        ProgressBar progressBar = new(1, "Processing data...", new ProgressBarOptions { BackgroundColor = ConsoleColor.DarkGray })
        {
            Message = $"Total rows to process: {t}",
            MaxTicks = t
        };

        while (true)
        {
            var r = _vectorizer.LoadData(_queueBatchSize, _queue);

            if (r == 0)
            {
                progressBar.Message = "No more data to process. Exiting...";
                break;
            }

            var childBar = progressBar.Spawn(r, $"Processing {r} rows batch...", new ProgressBarOptions { BackgroundColor = ConsoleColor.DarkGray, DisplayTimeInRealTime = false, CollapseWhenFinished = true });

            void updateProgress()
            {
                childBar.Tick();
                progressBar.Tick();                
                progressBar.Message = $"Total rows to process: {t} | Processed: {progressBar.CurrentTick} | Current Queue: {_queue.Count}";
            }

            try
            {
                Enumerable.Range(1, _maxTasks).ToList().ForEach(
                    n => tasks.Add(
                        new Task(() => GetEmbeddings(n, childBar, () => updateProgress()))
                        )
                    );
                tasks.ForEach(t => t.Start());
                Task.WaitAll([.. tasks]);                
            }
            catch (Exception e)
            {
                progressBar.WriteErrorLine("Error in one of the tasks. Terminating process.");
                exception = e;
                break;
            }

            tasks.Clear();

        };

        if (exception != null)
        {
            Console.WriteLine($"Error: {exception.Message}");
        }
    }

    private static void GetEmbeddings(int taskId, ChildProgressBar childBar, Action updateProgress)
    {
        System.Diagnostics.Debug.Assert(_vectorizer != null); 

        Random random = new();
        OpenAIClient openAIClient = _openAIClients[taskId % _openAIClients.Count];
        //Task.Delay((taskId - 1) * 1500).Wait();
        try
        {
            do
            {
                List<int> ids = [];
                var taskBar = childBar.Spawn(_openaiBatchSize, $"Task {taskId}", new ProgressBarOptions { BackgroundColor = ConsoleColor.DarkGray, DisplayTimeInRealTime = false, CollapseWhenFinished = true });

                // Start to dequeue the row to process and generate chunks for them
                // Create a batch that is closest to the maximum batch size.
                taskBar.Message = $"Task {taskId}: Dequeueing and Chunking...";
                List<ChunkedText> batch = [];
                while (_queue.TryDequeue(out EmbeddingData? data))
                {
                    if (data == null) continue;

                    var paragraphs = TextChunker.SplitPlainTextParagraphs([data.Text], 2048);
                    int chunkId = 0;
                    foreach (var paragraph in paragraphs)
                    {
                        chunkId += 1;
                        batch.Add(new ChunkedText(data.RowId, chunkId, paragraph));
                    }

                    if (batch.Count >= _openaiBatchSize) break;
                }                
                taskBar.MaxTicks = batch.Count;

                // Split the batch in smaller sets if size is greater than max batch size
                // (For example if one row generate a lot of chunks)
                int openAIBatchNumber = 1;
                int prevRowId = -1;
                int curRowId = -1;
                foreach (var bc in batch.OrderBy(o => o.RowId).ToList().Chunk(_openaiBatchSize))
                {
                    // Create the batch to be sent to Open AI
                    EmbeddingsOptions options = new() { DeploymentName = _embeddingModel };
                    foreach (var c in bc)
                    {
                        options.Input.Add(c.Text);
                    }

                    // Get embeddings for the batch
                    int attempts = 0;                    
                    string msgPrefix = $"Task {taskId} (B:{openAIBatchNumber}, T:{batch.Count})";
                    while (attempts < 3)
                    {
                        try
                        {
                            taskBar.Message = $"{msgPrefix}: Getting Embeddings...";
                            var returnValue = openAIClient.GetEmbeddings(options);

                            // Save embeddings to the database
                            taskBar.Message = $"{msgPrefix}: Saving Embeddings...";                            
                            foreach (var (item, index) in returnValue.Value.Data.Select((item, index) => (item, index)))
                            {
                                prevRowId = curRowId;
                                curRowId = bc[index].RowId;
                                _vectorizer.SaveEmbedding(curRowId, bc[index].Text, item.Embedding.ToArray());
                                taskBar.Tick();         
                                if (prevRowId != curRowId && prevRowId > -1) updateProgress();
                            }

                            attempts = int.MaxValue;
                        }
                        catch (RequestFailedException ex)
                        {
                            if (ex.ErrorCode == null) throw;
                            if (ex.ErrorCode.Contains("429"))
                            {
                                attempts += 1;
                                taskBar.Message = $"{msgPrefix}: Throttled ({attempts}).";
                                Task.Delay(2000).Wait();
                            }
                            else throw;
                        }
                    }

                    openAIBatchNumber += 1;
                }             
                updateProgress();                                                                           
            } while (!_queue.IsEmpty);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{taskId:00}] !Error! ErrorType:{ex.GetType()} Message:{ex.Message}");
            throw;
        }
    }
};

