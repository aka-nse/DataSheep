using System.Collections;

namespace DataSheep;

partial class DataFrame<TRecord>
{
    /// <summary>
    /// The mutable table whose each row can be mapped with <typeparamref name="TRecord"/>.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    public sealed class Mutable : DataFrame<TRecord>
    {
        private int _generation;

        private static ObjectDisposedException ObjectDisposedError() => new ("");

        internal new IMutableSeries[] Series
            => (IMutableSeries[])base.Series ?? throw ObjectDisposedError();
        internal IMutableSeries[]? _series;

        /// <summary>
        /// Gets or sets the record in the specified row.
        /// </summary>
        /// <param name="rowIndex"></param>
        /// <returns></returns>
        public new TRecord this[int rowIndex]
        {
            get => base[rowIndex];
            set
            {
                _buffer[0] = value;
                Trait.WriteToSeries(Series, rowIndex, _buffer);
                ++_generation;
            }
        }

        internal Mutable(IRecordTrait<TRecord> trait, IMutableSeries[] series)
            : base(trait, series)
        {
            _series = series;
        }

        /// <summary>
        /// Destroys this instance and gets immutable version of this.
        /// </summary>
        /// <returns></returns>
        public Immutable MoveToImmutable()
        {
            var series = Interlocked.Exchange(ref _series, null)
                ?? throw ObjectDisposedError();
            return new Immutable(Trait, series);
        }

        /// <inheritdoc />
        public override IEnumerable<TRecord> AsEnumerable()
            => new Enumerable(this);

        /// <summary>
        /// Sets bulk records to the specified rows.
        /// </summary>
        /// <param name="rowIndex"></param>
        /// <param name="source"></param>
        public void SetRecords(int rowIndex, ReadOnlySpan<TRecord> source)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(rowIndex, nameof(rowIndex));
            ArgumentOutOfRangeException.ThrowIfLessThan(source.Length, RowCount - rowIndex, nameof(source));
            Trait.WriteToSeries(Series, rowIndex, source);
            ++_generation;
        }

        /// <summary>
        /// Append the specified record at the tail of rows.
        /// </summary>
        /// <param name="record"></param>
        public void AddRecord(TRecord record)
            => InsertRecord(RowCount, record);

        /// <summary>
        /// Append the specified records at the tail of rows.
        /// </summary>
        /// <param name="records"></param>
        public void AddRecords(ReadOnlySpan<TRecord> records)
            => InsertRecords(RowCount, records);

        /// <summary>
        /// Inserts the specified record at the specified position of rows.
        /// </summary>
        /// <param name="rowIndex"></param>
        /// <param name="record"></param>
        public void InsertRecord(int rowIndex, TRecord record)
        {
            _buffer[0] = record;
            InsertRecords(rowIndex, _buffer);
        }

        /// <summary>
        /// Inserts the specified records at the specified position of rows.
        /// </summary>
        /// <param name="rowIndex"></param>
        /// <param name="records"></param>
        public void InsertRecords(int rowIndex, ReadOnlySpan<TRecord> records)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan((uint)rowIndex, (uint)RowCount, nameof(rowIndex));
            foreach(var series in Series)
            {
                series.Expand(rowIndex, records.Length);
            }
            Trait.WriteToSeries(Series, rowIndex, records);
            ++_generation;
        }

        /// <summary>
        /// Removes the record at the specified position of rows.
        /// </summary>
        /// <param name="rowIndex"></param>
        public void RemoveRecord(int rowIndex)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)rowIndex, (uint)RowCount, nameof(rowIndex));
            foreach(var series in Series)
            {
                series.Shrink(rowIndex, 1);
            }
            ++_generation;
        }

        /// <summary>
        /// Removes the records at the specified position of rows.
        /// </summary>
        /// <param name="rowIndex"></param>
        /// <param name="rowCount"></param>
        public void RemoveRecords(int rowIndex, int rowCount)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)rowIndex, (uint)RowCount, nameof(rowIndex));
            ArgumentOutOfRangeException.ThrowIfNegative(rowCount, nameof(rowCount));
            foreach(var series in Series)
            {
                series.Shrink(rowIndex, rowCount);
            }
            ++_generation;
        }

        /// <summary>
        /// Clears all records.
        /// </summary>
        public void Clear()
        {
            foreach(var series in Series)
            {
                series.Clear();
            }
            ++_generation;
        }


        private new class Enumerable : IEnumerable<TRecord>, IEnumerator<TRecord>
        {
            private Enumerable? _nextEnumerator;
            private readonly Mutable _owner;
            private int _generation;
            private int _rowIndex = -1;

            public Enumerable(Mutable owner)
            {
                _owner = owner;
                _nextEnumerator = this;
            }

            public TRecord Current => (uint)_rowIndex < (uint)_owner.RowCount
                ? _owner[_rowIndex]
                : throw new InvalidOperationException();
            object IEnumerator.Current => Current!;


            public bool MoveNext()
            {
                if(_generation != _owner._generation)
                {
                    throw new InvalidOperationException();
                }
                ++_rowIndex;
                return _rowIndex < _owner.RowCount;
            }

            public void Dispose() { }

            public void Reset() => _rowIndex = -1;

            public IEnumerator<TRecord> GetEnumerator()
            {
                if(Interlocked.Exchange(ref _nextEnumerator, null) is { } nextEnumerator)
                {
                    nextEnumerator._generation = _owner._generation;
                    return nextEnumerator;
                }
                return new Enumerable(_owner) { _generation = _owner._generation };
            }
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }
}