using System.Collections;
using System.Runtime.CompilerServices;

namespace DataSheep;

partial class DataFrame
{
    public static DataFrame<TRecord> Create<TRecord>(IReadOnlyList<TRecord> records)
        where TRecord : IDataRecord<TRecord>
        => MutableDataFrame.Create(records).MoveToImmutable();

    public static DataFrame<TRecord> Create<TRecord>(ReadOnlySpan<TRecord> records)
        where TRecord : IDataRecord<TRecord>
        => MutableDataFrame.Create(records).MoveToImmutable();

    public static DataFrame<TTuple> Create<TTuple>(IReadOnlyList<TTuple> records, params string[] columnNames)
        where TTuple : struct,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            IStructuralComparable,
            IStructuralEquatable,
            ITuple
        => MutableDataFrame.Create(records, columnNames).MoveToImmutable();

    public static DataFrame<TTuple> Create<TTuple>(ReadOnlySpan<TTuple> records, params string[] columnNames)
        where TTuple : struct,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            IStructuralComparable,
            IStructuralEquatable,
            ITuple
        => MutableDataFrame.Create(records, columnNames).MoveToImmutable();
}
