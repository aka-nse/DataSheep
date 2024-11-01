using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;


partial class DataFrame<TRecord>
{
    /// <summary>
    /// The immutable table whose each row can be mapped with <typeparamref name="TRecord"/>.
    /// </summary>
    public sealed class Immutable : DataFrame<TRecord>
    {
        internal Immutable(IRecordTrait<TRecord> trait, ISeries[] series)
            : base(trait, series)
        {
        }

        /// <inheritdoc />
        public override Immutable CopyAsImmutable() => this;
    }
}
