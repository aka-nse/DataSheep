namespace DataSheep.Linq;

partial class DataFrameLinq
{
    public static DataFrame<TResult>.ReadOnly Select<TSource, TResult>(
        this DataFrame<TSource> source,
        Func<TSource, TResult> predicate)
    {
        ArgumentNullException.ThrowIfNull(source, nameof(source));
        ArgumentNullException.ThrowIfNull(predicate, nameof(predicate));
        throw new NotImplementedException();
    }
}
