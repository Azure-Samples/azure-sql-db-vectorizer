using System;
using Microsoft.Data.SqlClient;
using Dapper;
using System.Collections.Concurrent;
using System.Linq;

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

    public void InitializeDatabase()
    {
        using SqlConnection conn = new(_connectionString);
        
        var c = conn.ExecuteScalar<int>($"""
            drop table if exists [dbo].[dedicated_table_vectorizer];
            create table [dbo].[dedicated_table_vectorizer]
            (
                id int identity(1,1) primary key nonclustered,
                parent_id int not null,
                {_tableInfo.EmbeddingColumn} varbinary(8000) not null
            );            
            create clustered index [ixc] on [dbo].[dedicated_table_vectorizer](parent_id)
        """);
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
                [dbo].[dedicated_table_vectorizer] e on s.{_tableInfo.IdColumn} = e.parent_id
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
                    [dbo].[dedicated_table_vectorizer] e on s.{_tableInfo.IdColumn} = e.parent_id
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
                [dbo].[dedicated_table_vectorizer] (parent_id, {_tableInfo.EmbeddingColumn})
            values
                (@id, json_array_to_vector(@e))
        """, new { @id, @e } );
    }
}