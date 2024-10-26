namespace DataSheep;

public interface IDataFrame<TRecord>
{
    public int RowCount { get; }

    public TRecord this[int rowIndex] { get; }

    public void GetRecords(int rowIndex, Span<TRecord> destination);

    public TRecord[] GetRecords()
    {
        var records = new TRecord[RowCount];
        GetRecords(0, records);
        return records;
    }

    public IEnumerable<TRecord> AsEnumerable();

    public MutableDataFrame<TRecord> CopyAsMutable();

    public DataFrame<TRecord> CopyAsImmutable();

    public DataFrame<TRecord> MoveAsImmutable();
}
