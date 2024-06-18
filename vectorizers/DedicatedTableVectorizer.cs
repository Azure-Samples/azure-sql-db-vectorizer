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

public class DedicatedTableVectorizer: IVectorizer
{
    private readonly string _connectionString;

    private readonly TableInfo _tableInfo;

    public DedicatedTableVectorizer(string connectionString, TableInfo tableInfo)
    {
        _connectionString = connectionString;
        _tableInfo = tableInfo;        
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
                {_tableInfo.Table} s 
            left join
                [dbo].[wikipedia_articles_embeddings_title_embeddings] e on s.id = e.parent_id
            where
                e.parent_id is null    
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
                    s.{_tableInfo.IdColumn},
                    s.{_tableInfo.TextColumn}
                from 
                    {_tableInfo.Table} as s 
                left join
                    [dbo].[wikipedia_articles_embeddings_title_embeddings] e on s.id = e.parent_id
                where
                    e.parent_id is null 
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
            insert into
                [dbo].[wikipedia_articles_embeddings_title_embeddings] (parent_id, embedding)
            values
                (@id, json_array_to_vector(@e))
        """, new { @id, @e } );
    }
}