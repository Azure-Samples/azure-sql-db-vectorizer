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

public class SameTableVectorizer: IVectorizer
{
    private readonly string _connectionString;

    private readonly TableInfo _tableInfo;

    public SameTableVectorizer(string connectionString, TableInfo tableInfo)
    {
        _connectionString = connectionString;
        _tableInfo = tableInfo;        
    }

    public void InitializeDatabase()
    {
        // Nothing do to here
    }

    public void TestConnection()
    {
        using SqlConnection conn = new(_connectionString);
        conn.Open();
        conn.Close();
    }

    public int GetDataCount()
    {
        using SqlConnection conn = new(_connectionString);
        
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

    public int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue)
    {
        try
        {
            using SqlConnection conn = new(_connectionString);

            var reader = conn.ExecuteReader($"""
                select top({queueBatchSize})
                    t.{_tableInfo.IdColumn},
                    t.{_tableInfo.TextColumn}
                from 
                    {_tableInfo.Table} as t
                where 
                    {_tableInfo.EmbeddingColumn} is null     
            """);            

            while (reader.Read())
            {
                int id = reader.GetInt32(0);
                string text = reader.GetString(1);
                queue.Enqueue(new EmbeddingData(id, text));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            throw;
        }

        return queue.Count;
    }

    public void SaveEmbedding(int id, float[] embedding) {
        var e = "[" + string.Join(",", embedding.ToArray()) + "]";

        using SqlConnection conn = new(_connectionString);
        conn.Execute($"""
            update
                {_tableInfo.Table} 
            set    
                {_tableInfo.EmbeddingColumn} = json_array_to_vector(@e)
            where
                {_tableInfo.IdColumn} = @id
        """, new { @e, @id } );
    }
}