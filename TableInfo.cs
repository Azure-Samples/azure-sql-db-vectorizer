using System.Linq;

namespace azure_sql_db_data_to_embeddings;

public class TableInfo(string name, string idColumn, string textColumn, string embeddingColumn)
{
    public readonly string Table = SanitizeObjectName(name);
    public readonly string IdColumn = SanitizeObjectName(idColumn);
    public readonly string TextColumn = SanitizeObjectName(textColumn);
    public readonly string EmbeddingColumn = SanitizeObjectName(embeddingColumn);

    private static string SanitizeObjectName(string name)
    {
        string result = string.Empty;

        var tname = name.Trim();
        if (tname.StartsWith('[') && tname.EndsWith(']'))
        {
            result =  tname;
        }

        var nameParts =  tname.Split('.');
        foreach((var np, var i) in nameParts.Select((np, i) => (np, i)))
        {
            if (!(np.Trim().StartsWith('[') || np.Trim().StartsWith(']')))
            {
                nameParts[i] = "[" + np + "]";
            }
        }

        result = string.Join(".", nameParts);      
        //Console.WriteLine($"Sanitized: {result}");
        return result;    
    }
}

