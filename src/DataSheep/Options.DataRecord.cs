namespace DataSheep;

public record class DataRecordOptions(
    IReadOnlyDictionary<int, string> ColumnNames)
{
    public static DataRecordOptions Default { get; }
        = new([]);

    public DataRecordOptions(IReadOnlyList<string> ColumnNames)
        : this(MapColumnNamesFromList(ColumnNames))
    {
    }

    private static IReadOnlyDictionary<int, string> MapColumnNamesFromList(IReadOnlyList<string> ColumnNames)
    {
        var dict = new Dictionary<int, string>();
        for(var i = 0; i < ColumnNames.Count; ++i)
        {
            dict[i] = ColumnNames[i];
        }
        return dict;
    }
}
