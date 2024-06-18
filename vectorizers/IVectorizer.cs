using System.Collections.Concurrent;

namespace azure_sql_db_data_to_embeddings;

public record EmbeddingData(int Id, string Text);

public interface IVectorizer
{
    public void TestConnection();

    public void InitializeDatabase();

    public int GetDataCount();

    public int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue);

    public void SaveEmbedding(int id, float[] embedding);
}   