using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep.Linq;

partial class QueryableDataFrame
{
}

file class SelectDataFrame<TSource, TResult>(DataFrame<TSource> source)
    where TSource : ITuple
    where TResult : ITuple
{

}
