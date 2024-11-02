using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

partial class DataFrame<TRecord>
{
    /// <summary>
    /// The readonly but not necessarily immutable table whose each row can be mapped with <typeparamref name="TRecord"/>.
    /// </summary>
    public sealed class ReadOnly(IRecordTrait<TRecord> trait, ISeries[] series)
        : DataFrame<TRecord>(trait, series)
    {
    }
}
