using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DataSheep;

public class LazySeriesChunkTest
{
    [Fact]
    public void Fact()
    {
        var series = new MutableSeries<byte>("", 256);
        series.Expand(0, 256);
        var chunk = new LazySeries.Chunk(0, series);
        var eval = new Evaluator();
        chunk.GetValues<byte>(eval, 70, new byte[20]);
        chunk.GetValues<byte>(eval, 140, new byte[20]);
        chunk.GetValues<byte>(eval, 210, new byte[20]);
        chunk.GetValues<byte>(eval, 0, new byte[200]);
    }

    public static string ToString(MutableSeries<byte> series)
    {
        var sb = new StringBuilder();
        for(int i = 0; i < series.Count; i++)
        {
            sb.Append(series[i]);
        }
        sb.AppendLine();
        for(int i = 0; i < series.Count; i++)
        {
            sb.Append(i % 10 == 0 ? '^' : ' ');
        }
        sb.AppendLine();
        for(int i = 0; i < series.Count; i++)
        {
            if(i % 10 != 0)
            {
                sb.Append(' ');
                continue;
            }
            var s = i.ToString();
            sb.Append(s);
            i += s.Length - 1;
        }
        return sb.ToString();
    }
}


file class Evaluator : ILazyEvaluator<byte>
{
    public ReadOnlySpan<byte> AccessTimes => _AccessTimes;
    private readonly byte[] _AccessTimes = new byte[256];
    public void EvaluateValues(int startRowIndex, Span<byte> destination)
    {
        for(int i = 0; i < destination.Length; i++)
        {
            ++_AccessTimes[startRowIndex + i];
        }
        destination.Fill(1);
    }

    public override string ToString()
    {
        var sb = new StringBuilder();
        for(int i = 0; i < _AccessTimes.Length; i++)
        {
            sb.Append(_AccessTimes[i]);
        }
        sb.AppendLine();
        for(int i = 0; i < _AccessTimes.Length; i++)
        {
            sb.Append(i % 10 == 0 ? '^' : ' ');
        }
        sb.AppendLine();
        for(int i = 0; i < _AccessTimes.Length; i++)
        {
            if(i % 10 != 0)
            {
                sb.Append(' ');
                continue;
            }
            var s = i.ToString();
            sb.Append(s);
            i += s.Length - 1;
        }
        return sb.ToString();
    }
}