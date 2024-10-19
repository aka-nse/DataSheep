using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

partial class MutableDataFrame
{
    public static MutableDataFrame<TRecord> Create<TRecord>(int initialCapacity = 256)
        where TRecord : IDataRecord<TRecord>
        => new MutableDataFrame<TRecord, DataRecordStrategy<TRecord>>(initialCapacity, TRecord.Columns);

    public static MutableDataFrame<TRecord> Create<TRecord>(IReadOnlyList<TRecord> records)
        where TRecord : IDataRecord<TRecord>
    {
        var df = new MutableDataFrame<TRecord, DataRecordStrategy<TRecord>>(records.Count, TRecord.Columns);
        using var temp = TemporaryBuffer.Create(records);
        df.AddRange(temp.Span);
        return df;
    }

    public static MutableDataFrame<TRecord> Create<TRecord>(ReadOnlySpan<TRecord> records)
        where TRecord : IDataRecord<TRecord>
    {
        var df = new MutableDataFrame<TRecord, DataRecordStrategy<TRecord>>(records.Length, TRecord.Columns);
        df.AddRange(records);
        return df;
    }

    public static MutableDataFrame<TTuple> Create<TTuple>(int initialCapacity = 256, params string[] columnNames)
        where TTuple : struct,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            IStructuralComparable,
            IStructuralEquatable,
            ITuple
        => RecordStrategy.CreateMutableDataFrame<TTuple>(initialCapacity, columnNames);


    public static MutableDataFrame<TTuple> Create<TTuple>(IReadOnlyList<TTuple> records, params string[] columnNames)
        where TTuple : struct,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            IStructuralComparable,
            IStructuralEquatable,
            ITuple
    {
        var df = Create<TTuple>(records.Count, columnNames);
        using var temp = TemporaryBuffer.Create(records);
        df.AddRange(temp.Span);
        return df;
    }

    public static MutableDataFrame<TTuple> Create<TTuple>(ReadOnlySpan<TTuple> records, params string[] columnNames)
        where TTuple : struct,
            IComparable,
            IComparable<TTuple>,
            IEquatable<TTuple>,
            IStructuralComparable,
            IStructuralEquatable,
            ITuple
    {
        var df = Create<TTuple>(records.Length, columnNames);
        df.AddRange(records);
        return df;
    }
}
