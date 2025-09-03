using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using DotNetEnv;
using System;
using Dapper;

namespace Azure.SQL.DB.Vectorizer;

public record EmbeddingData(int RowId, string Text);

public interface IVectorizer
{
    public void TestConnection();

    public void InitializeDatabase();

    public int GetDataCount();

    public int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue);

    public void SaveEmbedding(int id, string text, ReadOnlyMemory<float> embedding);
}   

public abstract class BaseVectorizer: IVectorizer
{
    protected readonly string ConnectionString = Env.GetString("MSSQL_CONNECTION_STRING");

    public void TestConnection()
    {        
        using SqlConnection conn = new(ConnectionString);
        conn.Open();
        var serverInfo = conn.QuerySingle("select @@SERVERNAME as server_name, DB_NAME() as [database_name]");
        Console.WriteLine($"Connected to {serverInfo.server_name} database {serverInfo.database_name}.");
        conn.Close();
    }

    public virtual void InitializeDatabase() {}

    public abstract int GetDataCount();

    public abstract int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue);

    public abstract void SaveEmbedding(int id, string text, ReadOnlyMemory<float> embedding);
}   