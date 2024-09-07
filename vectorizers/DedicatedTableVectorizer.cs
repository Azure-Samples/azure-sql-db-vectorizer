using System;
using Microsoft.Data.SqlClient;
using Dapper;
using System.Collections.Concurrent;
using System.Linq;
using DotNetEnv;
using System.Data;

namespace Azure.SQL.DB.Vectorizer;


public class DedicatedTableInfo() : BaseTableInfo
{
    public readonly bool AutoCreateTableIfNotExists = bool.Parse(Env.GetString("AUTO_CREATE_DEDICATED_EMBEDDINGS_TABLE") ?? "false");

    public readonly string DedicatedEmbeddingsTable = SanitizeObjectName(Env.GetString("DEDICATED_EMBEDDINGS_TABLE") ?? "[dbo].[dedicated_table_vectorizer]");

    public readonly string ParentIdColumnname = SanitizeObjectName(Env.GetString("PARENT_ID_COLUMN_NAME") ?? "parent_id");
}


public class DedicatedTableVectorizer : BaseVectorizer
{
    private readonly string _connectionString = Env.GetString("MSSQL_CONNECTION_STRING");
    private readonly int _dimensions = Env.GetInt("EMBEDDING_DIMENSIONS");
    private readonly bool _saveTextChunks = Env.GetBool("SAVE_TEXT_CHUNKS");

    private readonly DedicatedTableInfo _tableInfo = new();

    public override void InitializeDatabase()
    {
        Console.WriteLine($"Save text chunks: {_saveTextChunks}");

        using SqlConnection conn = new(_connectionString);

        var c = conn.ExecuteScalar<int>($"""
            select 
                count(*)
            from
                sys.tables
            where
                [object_id] = object_id('{_tableInfo.DedicatedEmbeddingsTable}')
        """);
        var tableExists = c == 1;

        if (!tableExists)
        {
            if (_tableInfo.AutoCreateTableIfNotExists)
            {
                string textColumn = string.Empty;
                if (_saveTextChunks) 
                    textColumn = "chunk_text nvarchar(max) null,";
                
                Console.WriteLine($"Creating {_tableInfo.DedicatedEmbeddingsTable} table...");
                conn.ExecuteScalar<int>($"""
                create table {_tableInfo.DedicatedEmbeddingsTable}
                (
                    id int identity(1,1) primary key nonclustered,
                    {_tableInfo.ParentIdColumnname} int not null,
                    {textColumn}
                    {_tableInfo.EmbeddingColumn} vector({_dimensions}) not null
                );            
                create clustered index [ixc] on {_tableInfo.DedicatedEmbeddingsTable}({_tableInfo.ParentIdColumnname});
                create nonclustered index ix__review_id on {_tableInfo.DedicatedEmbeddingsTable} ({_tableInfo.ParentIdColumnname}, id);
            """);
            }
            else
            {
                throw new Exception($"Table {_tableInfo.DedicatedEmbeddingsTable} does not exist");
            }
        }
        else
        {
            Console.WriteLine($"Table {_tableInfo.DedicatedEmbeddingsTable} found...");
        }
    }

    public override int GetDataCount()
    {
        string sql = $"""
            with cte as (
                select 
                    s.{_tableInfo.IdColumn}
                from
                    {_tableInfo.Table} as s 
                left join
                    {_tableInfo.DedicatedEmbeddingsTable} e on e.{_tableInfo.ParentIdColumnname} = s.{_tableInfo.IdColumn}
                where
                    e.id is null
            )
            select count(r.{_tableInfo.IdColumn}) from {_tableInfo.Table} as r 
            where exists (select * from cte c where c.{_tableInfo.IdColumn} = r.id)
        """;

        using SqlConnection conn = new(_connectionString);
        var c = conn.ExecuteScalar<int>(sql);

        return c;
    }

    public override int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue)
    {
        string sql = $"""
                with cte as (
                    select top({queueBatchSize})
                        s.{_tableInfo.IdColumn}
                    from
                        {_tableInfo.Table} as s 
                    left join
                        {_tableInfo.DedicatedEmbeddingsTable} e on e.{_tableInfo.ParentIdColumnname} = s.{_tableInfo.IdColumn}
                    where
                        e.id is null
                )
                select r.{_tableInfo.IdColumn}, r.{_tableInfo.TextColumn} from {_tableInfo.Table} as r 
                where exists (select * from cte c where c.{_tableInfo.IdColumn} = r.id)
            """;

        try
        {
            using SqlConnection conn = new(_connectionString);
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

    public override void SaveEmbedding(int id, string text, float[] embedding)
    {
        var e = "[" + string.Join(",", embedding.ToArray()) + "]";

        using SqlConnection conn = new(_connectionString);
        if (_saveTextChunks)
        {
            conn.Execute($"""
                insert into
                    {_tableInfo.DedicatedEmbeddingsTable} ({_tableInfo.ParentIdColumnname}, chunk_text, {_tableInfo.EmbeddingColumn})
                values
                    (@id, @t, cast(@e as vector({_dimensions})))
            """, new { @id, @t = text, @e });
        } else {
            conn.Execute($"""
                insert into
                    {_tableInfo.DedicatedEmbeddingsTable} ({_tableInfo.ParentIdColumnname}, {_tableInfo.EmbeddingColumn})
                values
                    (@id, cast(@e as vector({_dimensions})))
            """, new { @id, @e });
        }
    }
}