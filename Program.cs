using Azure.AI.OpenAI;
using Azure.Identity;
using DotNetEnv;
using Microsoft.SemanticKernel.Text;
using OpenAI.Embeddings;
using ShellProgressBar;
using System;
using System.ClientModel;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using static System.Net.WebRequestMethods;

#pragma warning disable SKEXP0050

namespace Azure.SQL.DB.Vectorizer;

public static class Defaults
{
    public static readonly int EmbeddingDimensions = 1536;
}

public class EmbeddingConfiguration
{
    public bool ChunkText { get; init; }
    public int Dimensions { get; init; }
    public int ChunkMaxLength { get; init; }

    public EmbeddingConfiguration(int dimensions, bool chunkText, int chunkMaxLength)
    {
        ChunkText = chunkText;
        Dimensions = dimensions;
        ChunkMaxLength = chunkMaxLength;
    }
}

public class Program
{

    private static readonly int _queueBatchSize = 5000;
    private static readonly ConcurrentQueue<EmbeddingData> _queue = new();

    private static readonly List<Task> tasks = [];
    private static int _maxTasks = 0; // 0 to auto-detect

    private static readonly List<EmbeddingClient> _embeddingClients = [];
    private static readonly int _openaiBatchSize = 50;
    private static string _embeddingModel = "text-embedding-3-small";

    private static IVectorizer? _vectorizer; 
    private record ChunkedText(int RowId, int ChunkId, string Text);

    static void Main(string[] args)
    {
        if (Environment.UserInteractive && !System.Diagnostics.Debugger.IsAttached)
            Console.Clear();

        var assembly = Assembly.GetExecutingAssembly();
        var version = assembly.GetName().Version;
        
        Console.WriteLine($"Azure SQL DB Vectorizer v{version}");
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
            _vectorizer = new SameTableVectorizer();
        }

        _embeddingModel = Env.GetString("OPENAI_EMBEDDING_DEPLOYMENT_NAME");

        string oaiUrls = Env.GetString("OPENAI_URL");
        string oaiKeys = Env.GetString("OPENAI_KEY") ?? string.Empty;

        string[] _oaiEndpoint = oaiUrls.Split(",");

        if (string.IsNullOrEmpty(oaiKeys))
        {
            Console.WriteLine("No OPENAI_KEY provided. Using DefaultAzureCredential.");
            oaiKeys = string.Join(",", Enumerable.Repeat(string.Empty, _oaiEndpoint.Length));
        }

        string[] _oaiKey = oaiKeys.Split(",");

        if (_oaiEndpoint.Length != _oaiKey.Length)
        {
            throw new ApplicationException("OpenAI URL and Key count mismatch.");
        }

        foreach (var (url, key) in _oaiEndpoint.Zip(_oaiKey))
        {
            AzureOpenAIClient azureClient = string.IsNullOrEmpty(key) switch
            {
               true => new AzureOpenAIClient(new Uri(url), new DefaultAzureCredential()),
               false => new AzureOpenAIClient(new Uri(url), new AzureKeyCredential(key))
            };
            
            EmbeddingClient embeddingClient = azureClient.GetEmbeddingClient(_embeddingModel);
            _embeddingClients.Add(embeddingClient);
        }

        _maxTasks = _maxTasks == 0 ? _embeddingClients.Count * 2 : _maxTasks;
        
        Console.WriteLine($"Embedding Clients: {_embeddingClients.Count}, Max Tasks: {_maxTasks}, Max Queue Size: {_queueBatchSize}, REST API Batch Size: {_openaiBatchSize}");
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
        Console.WriteLine($"Total rows to process: {t}");

        bool chunkText = _vectorizer is DedicatedTableVectorizer;
        Console.WriteLine($"Chunk text: {chunkText}");

        // Create embedding configuration
        int dimensions = Env.GetInt("EMBEDDING_DIMENSIONS", Defaults.EmbeddingDimensions);
        int chunkMaxLength = Env.GetInt("CHUNK_MAX_LENGTH", 2048);
        var embeddingConfig = new EmbeddingConfiguration(dimensions, chunkText, chunkMaxLength);
        Console.WriteLine($"Embedding dimensions: {embeddingConfig.Dimensions}, max tokens: {embeddingConfig.ChunkMaxLength}");

        Console.WriteLine("Running:");        
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
                        new Task(() => GetEmbeddings(n, embeddingConfig, childBar, () => updateProgress()))
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

    private static void GetEmbeddings(int taskId, EmbeddingConfiguration embeddingConfig, ChildProgressBar childBar, Action updateProgress)
    {
        System.Diagnostics.Debug.Assert(_vectorizer != null);

        Random random = new();
        EmbeddingClient embeddingClient = _embeddingClients[taskId % _embeddingClients.Count];
        //Task.Delay((taskId - 1) * 1500).Wait();
        try
        {
            do
            {
                List<int> ids = [];
                var taskBar = childBar.Spawn(_openaiBatchSize, $"Task {taskId}", new ProgressBarOptions { BackgroundColor = ConsoleColor.DarkGray, DisplayTimeInRealTime = false, CollapseWhenFinished = true });

                // Start to dequeue the rows to process and generate chunks for them
                // Create a batch that is closest to the maximum batch size.
                taskBar.Message = $"Task {taskId} | Dequeueing and Chunking...";
                List<ChunkedText> batch = [];
                int rowCount = 0;
                while (_queue.TryDequeue(out EmbeddingData? data))
                {
                    if (data == null) continue;
                    rowCount += 1;

                    var paragraphs = TextChunker.SplitPlainTextParagraphs([data.Text], embeddingConfig.ChunkMaxLength);
                    if (embeddingConfig.ChunkText)
                    { 
                        int chunkId = 0;
                        foreach (var paragraph in paragraphs)
                        {
                            chunkId += 1;
                            batch.Add(new ChunkedText(data.RowId, chunkId, paragraph));
                        }
                    } else
                    {
                        batch.Add(new ChunkedText(data.RowId, 0, paragraphs[0]));
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
                    List<string> inputTexts = bc.Select(c => c.Text).ToList();

                    // Get embeddings for the batch
                    int attempts = 0;
                    string msgPrefix = $"Task {taskId} | Rows {rowCount} | Chunks {batch.Count}";
                    while (attempts < 3)
                    {
                        try
                        {
                            taskBar.Message = $"{msgPrefix} | Getting Embeddings (d={embeddingConfig.Dimensions})...";
                            var returnValue = embeddingClient.GenerateEmbeddings(inputTexts, new EmbeddingGenerationOptions() { Dimensions = embeddingConfig.Dimensions});

                            // Save embeddings to the database
                            taskBar.Message = $"{msgPrefix} | Saving Embeddings...";                            
                            foreach (var (embedding, index) in returnValue.Value.Select((embedding, index) => (embedding, index)))
                            {
                                var properEmbedding = embedding.ToFloats();

                                if (properEmbedding.Length != embeddingConfig.Dimensions)
                                    throw new ApplicationException($"Unexpected embedding dimensions {properEmbedding.Length} (expected {embeddingConfig.Dimensions})");

                                prevRowId = curRowId;
                                curRowId = bc[index].RowId;
                                _vectorizer.SaveEmbedding(curRowId, bc[index].Text, properEmbedding);
                                taskBar.Tick();
                                if (prevRowId != curRowId && prevRowId > -1) updateProgress();
                            }

                            attempts = int.MaxValue;
                        }
                        catch (Exception ex) when (IsThrottledException(ex))
                        {
                            attempts += 1;
                            taskBar.Message = $"{msgPrefix} | Throttled ({attempts}).";
                            Task.Delay(10000).Wait();
                        }                       
                    }

                    openAIBatchNumber += 1;
                }             
                updateProgress();                                                                           
            } while (!_queue.IsEmpty);
        }
        catch (Exception ex)
        {
            Console.Clear();
            Console.WriteLine($"[{taskId:00}] !Error! ErrorType:{ex.GetType()} Message:{ex.Message}");
            Environment.Exit(1);
        }
    }

    private static bool IsThrottledException(Exception ex)
    {
        return ex switch
        {
            RequestFailedException rfe when rfe.ErrorCode?.Contains("429") == true => true,
            ClientResultException cre when cre.Message.Contains("429") => true,
            _ => false
        };
    }
};

