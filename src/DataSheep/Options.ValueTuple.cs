namespace DataSheep;

public record class ValueTupleOptions(
    IReadOnlyDictionary<int, string> ColumnNames)
{
    public static ValueTupleOptions Default { get; }
        = new([]);

    public ValueTupleOptions(IReadOnlyList<string> ColumnNames)
        : this(MapColumnNamesFromList(ColumnNames))
    {
    }

    private static Dictionary<int, string> MapColumnNamesFromList(IReadOnlyList<string> ColumnNames)
    {
        var dict = new Dictionary<int, string>();
        for(var i = 0; i < ColumnNames.Count; ++i)
        {
            dict[i] = ColumnNames[i];
        }
        return dict;
    }
}
