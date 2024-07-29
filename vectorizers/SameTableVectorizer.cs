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

namespace Azure.SQL.DB.Vectorizer;

public class SameTableInfo: BaseTableInfo 
{}

public class SameTableVectorizer : BaseVectorizer
{
    private readonly SameTableInfo _tableInfo = new();

    public override int GetDataCount()
    {
        using SqlConnection conn = new(ConnectionString);
        
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

    public override int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue)
    {
        try
        {
            using SqlConnection conn = new(ConnectionString);

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

    public override void SaveEmbedding(int id, float[] embedding) {
        var e = "[" + string.Join(",", embedding.ToArray()) + "]";

        using SqlConnection conn = new(ConnectionString);
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