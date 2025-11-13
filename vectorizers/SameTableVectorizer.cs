using System;
using Microsoft.Data.SqlClient;
using Dapper;
using System.Collections.Concurrent;
using Microsoft.Data.SqlTypes;
using Microsoft.Data;
using System.Data;

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
            and
                trim({_tableInfo.TextColumn}) != '' 
            and
                {_tableInfo.TextColumn} is not null
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
            and
                trim({_tableInfo.TextColumn}) != '' 
            and
                {_tableInfo.TextColumn} is not null
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
        SqlParameter e = embedding.Length switch
        {
            > 1998 => new SqlParameter("@e", SqlDbType.VarChar) { Value = "[" + string.Join(",", embedding.ToArray()) + "]" },
            _ => new SqlParameter("@e", SqlDbTypeExtensions.Vector) { Value = new SqlVector<float>(embedding) }
        };

        using SqlConnection conn = new(ConnectionString);
        
        conn.Open();

        using SqlCommand command = new($"""
            update {_tableInfo.Table} 
            set {_tableInfo.EmbeddingColumn} = @e
            where {_tableInfo.IdColumn} = @id
            """, conn);
            command.Parameters.Add(new SqlParameter("@id", SqlDbType.Int) { Value = id });
            command.Parameters.Add(e);

        command.ExecuteNonQuery();
        
        conn.Close();
    }
}