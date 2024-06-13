using System;
using Azure;
using Azure.AI.OpenAI;
using Microsoft.Data.SqlClient;
using Dapper;
using DotNetEnv;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using ShellProgressBar;

namespace azure_sql_db_data_to_embeddings;

public record EmbeddingData(string Id, string Text);

public class Program
{
    static readonly TableInfo _tableInfo;
    static readonly string _embeddingModel;

    static readonly List<OpenAIClient> _openAIClients = [];
    static readonly int _openaiBatchSize = 25;

    static readonly int _queueBatchSize = 1000;
    static readonly ConcurrentQueue<EmbeddingData> _queue = new();

    static readonly List<Task> tasks = [];
    static readonly int _maxTasks = 0; // 0 to auto-detect
 
    static Program()
    {
        Env.Load(); 

        _tableInfo = new TableInfo
        (
            Env.GetString("TABLE_NAME"),
            Env.GetString("ID_COLUMN_NAME"),
            Env.GetString("CONTENT_COLUMN_NAME"),
            Env.GetString("EMBEDDING_COLUMN_NAME")
        );    

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

        Console.WriteLine("Connecting to database...");
        TestConnection();        

        Console.WriteLine("Getting rows count...");
        var t = GetDataCount();        

        Console.WriteLine("Processing rows...");
        ProgressBar progressBar = new(1, "Processing data...", new ProgressBarOptions { BackgroundColor = ConsoleColor.DarkGray })
        {
            Message = $"Total rows to process: {t}",
            MaxTicks = t
        };        

        while(true) {
            var r = LoadData();
            
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
                        new Task(() => GetEmbeddings(n, () => updateProgress()))
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

    private static void TestConnection()
    {
        using SqlConnection conn = new(Env.GetString("MSSQL_CONNECTION_STRING"));
        conn.Open();
        conn.Close();
    }

    private static void GetEmbeddings(int taskId, Action updateProgress)
    {
        Random random = new();
        OpenAIClient openAIClient = _openAIClients[taskId % _openAIClients.Count];
        Task.Delay(taskId * 500).Wait();
        try
        {            
            do 
            {
                EmbeddingsOptions options = new() { DeploymentName = _embeddingModel };
                List<string> ids = [];        

                // Prepare batch
                while (_queue.TryDequeue(out EmbeddingData? data))
                {
                    if (data == null) continue;
                    
                    options.Input.Add(data.Text);
                    ids.Add(data.Id);

                    if (options.Input.Count == _openaiBatchSize) break;                
                }

                // Get embeddings for the batch
                var returnValue = openAIClient.GetEmbeddings(options);
                    
                // Save embeddings to the database
                foreach (var (item, index) in returnValue.Value.Data.Select((item, index) => (item, index)))
                {
                    var e = "[" + string.Join(",", item.Embedding.ToArray()) + "]";
                    var id = ids[index];

                    using SqlConnection conn = new(Env.GetString("MSSQL_CONNECTION_STRING"));
                    conn.Execute($"""
                        update
                             {_tableInfo.Table} 
                        set    
                            {_tableInfo.EmbeddingColumn} = json_array_to_vector(@e)
                        where
                            {_tableInfo.IdColumn} = @id
                        """,
                        new { e, id }
                    );

                    updateProgress();
                }                
            } while (!_queue.IsEmpty);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{taskId:00}] Error: {ex.Message}");
            throw;
        }
    }

    private static int GetDataCount()
    {
        using SqlConnection conn = new(Env.GetString("MSSQL_CONNECTION_STRING"));
        
        var c = conn.ExecuteScalar<int>($"""
            select 
                count(*) 
            from 
                {_tableInfo.Table} 
            where 
                {_tableInfo.EmbeddingColumn} is null;            
        """);
        
        return c;
    }

    private static int LoadData()
    {
        try
        {
            using SqlConnection conn = new(Env.GetString("MSSQL_CONNECTION_STRING"));

            var reader = conn.ExecuteReader($"""
                select top({_queueBatchSize})
                    t.{_tableInfo.IdColumn},
                    t.{_tableInfo.TextColumn}
                from 
                    {_tableInfo.Table} as t
                where 
                    {_tableInfo.EmbeddingColumn} is null     
            """);            

            while (reader.Read())
            {
                string id = reader.GetInt32(0).ToString();
                string text = reader.GetString(1);
                _queue.Enqueue(new EmbeddingData(id, text));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            throw;
        }

        return _queue.Count;
    }
};

