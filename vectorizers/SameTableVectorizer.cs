using System;
using Microsoft.Data.SqlClient;
using Dapper;
using System.Collections.Concurrent;
using System.Linq;
using DotNetEnv;
using System.Data;
using Microsoft.Data.SqlTypes;

namespace Azure.SQL.DB.Vectorizer;

public class SameTableInfo() : BaseTableInfo;

public class SameTableVectorizer : BaseVectorizer
{
    private readonly SameTableInfo _tableInfo = new();

    public override void InitializeDatabase()
    {
        if (_tableInfo.Table == null)
        {
            throw new Exception("TABLE_NAME environment variable is not set");
        }

        if (_tableInfo.IdColumn == null)
        {
            throw new Exception("ID_COLUMN_NAME environment variable is not set");
        }   

        using SqlConnection conn = new(ConnectionString);

        var c = conn.ExecuteScalar<int>($"""
            select 
                count(*)
            from
                sys.tables
            where
                [object_id] = object_id('{_tableInfo.Table}')
        """);
        var tableExists = c == 1;

        if (!tableExists)
        {
            throw new Exception($"Table {_tableInfo.Table} does not exist");
        }
        else
        {
            Console.WriteLine($"Table {_tableInfo.Table} found...");
        }
    }

    public override int GetDataCount()
    {
        string sql = $"""
            select 
                count(*)                    
            from
                {_tableInfo.Table}         
            where
                {_tableInfo.EmbeddingColumn} is null
        """;

        using SqlConnection conn = new(ConnectionString);
        var c = conn.ExecuteScalar<int>(sql);

        return c;
    }

    public override int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue)
    {
        string sql = $"""
            select top({queueBatchSize})
                {_tableInfo.IdColumn},
                {_tableInfo.TextColumn}
            from
                {_tableInfo.Table} as s                    
            where
                {_tableInfo.EmbeddingColumn} is null
            """;

        try
        {
            using SqlConnection conn = new(ConnectionString);
            var reader = conn.ExecuteReader(sql);

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

    public override void SaveEmbedding(int id, string text, ReadOnlyMemory<float> embedding)
    {
        using SqlConnection conn = new(ConnectionString);

        DynamicParameters dynamicParameters = new();
        dynamicParameters.Add("@id", id);

        if (embedding.Length > 1998)
            dynamicParameters.Add("@e", "[" + string.Join(",", embedding.ToArray()) + "]");
        else
            dynamicParameters.Add("@e", new SqlVector<float>(embedding));

        conn.Execute($"""
            update {_tableInfo.Table} 
            set {_tableInfo.EmbeddingColumn} = @e
            where {_tableInfo.IdColumn} = @id
        """, dynamicParameters);
    }
}