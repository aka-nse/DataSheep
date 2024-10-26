using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DataSheep;

public interface IRecordTrait
{
    public int ColumnCount => DefaultColumnNames.Count;
    public IReadOnlyList<string> DefaultColumnNames { get; }
}

public interface IRecordTrait<T> : IRecordTrait
{
    public IMutableSeries[] CreateSeriesPrefab(int initialCapacity, IReadOnlyList<string> columnNames);
    public void ReadFromSeries(ReadOnlySpan<ISeries> series, int rowIndex, Span<T> destination);
    public void WriteToSeries(ReadOnlySpan<IMutableSeries> series, int rowIndex, ReadOnlySpan<T> source);
}

internal static class RecordTrait
{
    public class Cache<TRecord>
    {
        private static readonly Cache<TRecord> _instance;

        public static IRecordTrait<TRecord> Trait => _instance._trait;

        static Cache()
        {
            _instance = IsDataRecord<TRecord>()
                ? (Cache<TRecord>)Activator.CreateInstance(typeof(CacheForDataRecord<>).MakeGenericType([typeof(TRecord)]))!
                : new Cache<TRecord>();
        }

        private readonly IRecordTrait<TRecord> _trait;

        public Cache()
        {
            _trait = GetTrait();
        }

        public virtual IRecordTrait<TRecord> GetTrait()
            => (GetValueTupleTrait(typeof(TRecord)) as IRecordTrait<TRecord>)
            ?? new FallbackRecordTrait<TRecord>();
    }

    sealed class CacheForDataRecord<TRecord> : Cache<TRecord>
        where TRecord : IDataRecord<TRecord>
    {
        public override IRecordTrait<TRecord> GetTrait() => TRecord.Trait;
    }


    private static bool IsDataRecord<T>()
    {
        static bool isDataRecordInterface(Type ifType)
        {
            if(!ifType.IsGenericType) { return false; }
            if(ifType.GetGenericTypeDefinition() != typeof(IDataRecord<>)) { return false; }
            return ifType == typeof(IDataRecord<>).MakeGenericType(typeof(T));
        }

        return typeof(T).GetInterfaces().Any(isDataRecordInterface);
    }

    private static IRecordTrait? GetValueTupleTrait(Type tupleType)
    {
        static Type? getValueTupleTraitType(Type tupleType)
        {
            if(!tupleType.IsGenericType)
            {
                return null;
            }
            var genericDef = tupleType.GetGenericTypeDefinition();
            if(genericDef.Module != typeof(ValueTuple<>).Module)
            {
                return null;
            }
            if(genericDef.FullName?.Split('`')[0] != typeof(ValueTuple<>).FullName?.Split('`')[0])
            {
                return null;
            }
            var typeArgs = tupleType.GetGenericArguments();
            return typeArgs.Length switch
            {
                1 => typeof(ValueTupleTrait<>).MakeGenericType(typeArgs),
                2 => typeof(ValueTupleTrait<,>).MakeGenericType(typeArgs),
                3 => typeof(ValueTupleTrait<,,>).MakeGenericType(typeArgs),
                4 => typeof(ValueTupleTrait<,,,>).MakeGenericType(typeArgs),
                5 => typeof(ValueTupleTrait<,,,,>).MakeGenericType(typeArgs),
                6 => typeof(ValueTupleTrait<,,,,,>).MakeGenericType(typeArgs),
                7 => typeof(ValueTupleTrait<,,,,,,>).MakeGenericType(typeArgs),
                8 when getValueTupleTraitType(typeArgs[^1]) is { } rest => typeof(ValueTupleTrait<,,,,,,,,>).MakeGenericType([.. typeArgs, rest]),
                _ => null,
            };
        }

        var traitType = getValueTupleTraitType(tupleType);
        return traitType is { } ? (IRecordTrait)Activator.CreateInstance(traitType)! : null;
    }

}
