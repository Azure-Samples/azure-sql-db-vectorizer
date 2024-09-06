using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using DotNetEnv;

namespace Azure.SQL.DB.Vectorizer;

public record EmbeddingData(int Id, string Text);

public interface IVectorizer
{
    public void TestConnection();

    public void InitializeDatabase();

    public int GetDataCount();

    public int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue);

    public void SaveEmbedding(int id, float[] embedding);

    //public void SaveEmbedding(int id, string text, float[] embedding);
}   

public abstract class BaseVectorizer: IVectorizer
{
    protected readonly string ConnectionString = Env.GetString("MSSQL_CONNECTION_STRING");

    public void TestConnection()
    {
        using SqlConnection conn = new(ConnectionString);
        conn.Open();
        conn.Close();
    }

    public virtual void InitializeDatabase() {}

    public abstract int GetDataCount();

    public abstract int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue);

    public abstract void SaveEmbedding(int id, float[] embedding);
}   