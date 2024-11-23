namespace DataSheep;

internal static class Internalhelpers
{
    public static (int Offset, int Length) GetAvailableOffsetAndLength(in this Range range, int length)
    {
        var start = Math.Max(range.Start.GetOffset(length), 0);
        var end = Math.Min(range.End.GetOffset(length), length);
        return (start, Math.Max(0, end - start));
    }
}
