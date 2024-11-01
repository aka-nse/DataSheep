namespace DataSheep;

/// <summary>
/// Provides data frame APIs.
/// </summary>
/// <typeparam name="TRecord"></typeparam>
public interface IDataFrame<TRecord>
{
    /// <summary>
    /// Gets the current number of rows.
    /// </summary>
    public int RowCount { get; }

    /// <summary>
    /// Gets the record in the specified row.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <returns></returns>
    public TRecord this[int rowIndex] { get; }

    /// <summary>
    /// Gets bulk records in the specified rows.
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="destination"></param>
    public void GetRecords(int rowIndex, Span<TRecord> destination);

    /// <summary>
    /// Gets all records.
    /// </summary>
    /// <returns></returns>
    public TRecord[] GetRecords()
    {
        var records = new TRecord[RowCount];
        GetRecords(0, records);
        return records;
    }

    /// <summary>
    /// Gets an object which can enumerate all rows in this data frame.
    /// </summary>
    /// <returns></returns>
    public IEnumerable<TRecord> AsEnumerable();

    /// <summary>
    /// Creates a copy as mutable.
    /// </summary>
    /// <returns></returns>
    public DataFrame<TRecord>.Mutable CopyAsMutable();

    /// <summary>
    /// Creates a copy as immutable.
    /// </summary>
    /// <returns></returns>
    public DataFrame<TRecord>.Immutable CopyAsImmutable();
}
