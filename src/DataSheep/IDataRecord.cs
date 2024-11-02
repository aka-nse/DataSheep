namespace DataSheep;

/// <summary>
/// Expresses a specific named data record for data frame.
/// </summary>
/// <typeparam name="TSelf"></typeparam>
public interface IDataRecord<TSelf>
    where TSelf : IDataRecord<TSelf>
{
    /// <summary>
    /// Gets the trait instance that can handle <typeparamref name="TSelf"/> instance.
    /// </summary>
    public static abstract IRecordTrait<TSelf> Trait { get; }
}
