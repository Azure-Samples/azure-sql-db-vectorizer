using System.Linq;
using DotNetEnv;

namespace Azure.SQL.DB.Vectorizer;

public abstract class BaseTableInfo
{
    public readonly string Table = SanitizeObjectName(Env.GetString("TABLE_NAME"));
    public readonly string IdColumn = SanitizeObjectName(Env.GetString("ID_COLUMN_NAME"));
    public readonly string TextColumn = SanitizeObjectName(Env.GetString("CONTENT_COLUMN_NAME"));
    public readonly string EmbeddingColumn = SanitizeObjectName(Env.GetString("EMBEDDING_COLUMN_NAME"));

    protected static string SanitizeObjectName(string name)
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