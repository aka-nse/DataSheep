namespace DataSheep;

public interface IDataRecord<TSelf>
    where TSelf : IDataRecord<TSelf>
{
    public static abstract IRecordTrait<TSelf> Trait { get; }
}
