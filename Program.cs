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

#pragma warning disable SKEXP0050

namespace Azure.SQL.DB.Vectorizer;

public class Program
{
    static readonly string _embeddingModel;

    static readonly List<OpenAIClient> _openAIClients = [];
    static readonly int _openaiBatchSize = 50;

    static readonly int _queueBatchSize = 5000;
    static readonly ConcurrentQueue<EmbeddingData> _queue = new();

    static readonly List<Task> tasks = [];
    static readonly int _maxTasks = 0; // 0 to auto-detect

    static readonly IVectorizer vectorizer;
 
    static Program()
    {
        Env.Load(); 

        bool useDedicatedTable = !string.IsNullOrEmpty(Env.GetString("DEDICATED_EMBEDDINGS_TABLE"));

        if (useDedicatedTable)
            vectorizer = new DedicatedTableVectorizer();
        else
            vectorizer = new SameTableVectorizer();

        _embeddingModel = Env.GetString("OPENAI_EMBEDDING_DEPLOYMENT_NAME");

        string oaiUrls = Env.GetString("OPENAI_URL");
        string oaiKeys = Env.GetString("OPENAI_KEY");

        string[] _oaiEndpoint = oaiUrls.Split(",");
        string[] _oaiKey = oaiKeys.Split(",");

        if (_oaiEndpoint.Length != _oaiKey.Length)
        {
            throw new Exception("OpenAI URL and Key count mismatch.");
        }

        foreach (var (url, key) in _oaiEndpoint.Zip(_oaiKey))
        {
            AzureKeyCredential credentials = new(key);
            OpenAIClient openAIClient = new(new Uri(url), credentials);
            _openAIClients.Add(openAIClient);
        }

        _maxTasks = _maxTasks == 0 ? _openAIClients.Count * 2: _maxTasks;        
    }

    static void Main(string[] args)
    {
        Exception? exception = null;

        if (Environment.UserInteractive && !System.Diagnostics.Debugger.IsAttached)
            Console.Clear();               

        Console.WriteLine("Starting...");
        Console.WriteLine($"OpenAI Clients: {_openAIClients.Count}, Max Tasks: {_maxTasks}, Max Queue Size: {_queueBatchSize}, REST API Batch Size: {_openaiBatchSize}");

        Console.WriteLine($"Using {vectorizer.GetType()} vectorizer...");

        Console.WriteLine("Connecting to database...");
        vectorizer.TestConnection();                

        Console.WriteLine("Initializing database...");
        vectorizer.InitializeDatabase();                

        Console.WriteLine("Getting rows count...");
        var t = vectorizer.GetDataCount();        

        Console.WriteLine("Processing rows...");
        ProgressBar progressBar = new(1, "Processing data...", new ProgressBarOptions { BackgroundColor = ConsoleColor.DarkGray })
        {
            Message = $"Total rows to process: {t}",
            MaxTicks = t
        };                    

        while(true) {
            var r = vectorizer.LoadData(_queueBatchSize, _queue);
            
            if (r == 0) {
                progressBar.Message = "No more data to process. Exiting...";    
                break;  
            }

            var childBar = progressBar.Spawn(r, $"Processing {r} rows batch...", new ProgressBarOptions { BackgroundColor = ConsoleColor.DarkGray, DisplayTimeInRealTime = false, CollapseWhenFinished = true});

            void updateProgress()
            {
                progressBar.Message = $"Total rows to process: {t} | Processed: {progressBar.CurrentTick + 1} | Current Queue: {_queue.Count}";
                progressBar.Tick();
                childBar.Tick();
            }

            try {
                Enumerable.Range(1, _maxTasks).ToList().ForEach(
                    n => tasks.Add(
                        new Task(() => GetEmbeddings(n, childBar, () => updateProgress()))
                        )
                    );
                tasks.ForEach(t => t.Start()); 
                Task.WaitAll([.. tasks]);
            }
            catch(Exception e)
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

        Console.WriteLine("Done.");
    }

    private static void GetEmbeddings(int taskId, ChildProgressBar childBar, Action updateProgress)
    {        
        Random random = new();
        OpenAIClient openAIClient = _openAIClients[taskId % _openAIClients.Count];
        Task.Delay((taskId-1) * 1500).Wait();
        try
        {            
            do 
            {
                EmbeddingsOptions options = new() { DeploymentName = _embeddingModel };
                List<int> ids = [];        
                var taskBar = childBar.Spawn(_openaiBatchSize, $"Task {taskId}", new ProgressBarOptions { BackgroundColor = ConsoleColor.DarkGray, DisplayTimeInRealTime = false, CollapseWhenFinished = true});

                // Prepare batch
                taskBar.Message = $"Task {taskId}: Dequeueing and Chunking...";
                while (_queue.TryDequeue(out EmbeddingData? data))
                {
                    if (data == null) continue;

                    var paragraphs =  TextChunker.SplitPlainTextParagraphs([data.Text], 4000);
                    
                    // only get first chunk for now
                    options.Input.Add(paragraphs[0]);
                    ids.Add(data.Id);

                    // TODO process and store all the chunks, creating multiple batches if needed
                    // foreach (var paragraph in paragraphs)
                    // {
                    //     options.Input.Add(paragraph);
                    //     ids.Add(data.Id);
                    // }

                    if (options.Input.Count >= _openaiBatchSize) break;                
                }
                
                // Get embeddings for the batch
                int attempts = 0;
                while (attempts < 3)
                {
                    try {
                        taskBar.Message = $"Task {taskId}: Getting Embeddings...";
                        var returnValue = openAIClient.GetEmbeddings(options);                                    

                        // Save embeddings to the database
                        taskBar.Message = $"Task {taskId}: Saving Embeddings...";
                        foreach (var (item, index) in returnValue.Value.Data.Select((item, index) => (item, index)))
                        {   
                            vectorizer.SaveEmbedding(ids[index], item.Embedding.ToArray());
                            updateProgress();
                            taskBar.Tick();
                        }                

                        attempts = int.MaxValue;
                    } 
                    catch (RequestFailedException ex) {
                        if (ex.ErrorCode == null) throw;
                        if (ex.ErrorCode.Contains("429")) 
                        {
                            attempts += 1;
                            taskBar.Message = $"Task {taskId}: Throttled ({attempts}).";
                            Task.Delay(2000).Wait();
                        }
                        else throw;
                    }
                }

            } while (!_queue.IsEmpty);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{taskId:00}] !Error! ErrorType:{ex.GetType()} Message:{ex.Message}");            
            throw;
        }
    }
};

