using System.Collections.Concurrent;

namespace azure_sql_db_data_to_embeddings;

public record EmbeddingData(int Id, string Text);

public interface IVectorizer
{
    void TestConnection();

    int GetDataCount();

    int LoadData(int queueBatchSize, ConcurrentQueue<EmbeddingData> queue);

    void SaveEmbedding(int id, float[] embedding);
}   