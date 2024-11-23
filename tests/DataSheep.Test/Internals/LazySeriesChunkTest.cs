using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DataSheep;

public class LazySeriesChunkTest
{
    private static void AssertSpan(ReadOnlySpan<byte> actual, byte expectedValue, ReadOnlySpan<Range> expectedFilled)
    {
        var expected = (stackalloc byte[actual.Length]);
        foreach(var range in expectedFilled)
        {
            var (start, length) = range.GetAvailableOffsetAndLength(actual.Length);
            var end = start + length;
            for(var i = start; i < end; ++i)
            {
                expected[i] = expectedValue;
            }
        }
        Assert.Equal(expected, actual);
    }

    private static void AssertSeries(MutableSeries<byte> series, ReadOnlySpan<Range> expectedFilled)
    {
        var actual = (stackalloc byte[series.Count]);
        series.GetValues(0, actual);
        AssertSpan(actual, Evaluator.Value, expectedFilled);
    }


    [Fact]
    public void EnsureRecordsEvaluatedOnce()
    {
        var series = new MutableSeries<byte>("", 256);
        series.Expand(0, 256);
        var chunk = new LazySeries.Chunk(0, series);
        var eval = new Evaluator(256);

        Assert.Equal(20, chunk.GetValues<byte>(eval, 70, new byte[20]));
        AssertSeries(series, [70..90]);
        AssertSpan(eval.AccessTimes, 1, [70..90]);

        Assert.Equal(20, chunk.GetValues<byte>(eval, 140, new byte[20]));
        AssertSeries(series, [70..90, 140..160]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160]);

        Assert.Equal(20, chunk.GetValues<byte>(eval, 210, new byte[20]));
        AssertSeries(series, [70..90, 140..160, 210..230]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160, 210..230]);

        Assert.Equal(30, chunk.GetValues<byte>(eval, 50, new byte[30]));
        AssertSeries(series, [70..90, 140..160, 210..230, 50..80]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160, 210..230, 50..80]);

        Assert.Equal(30, chunk.GetValues<byte>(eval, 150, new byte[30]));
        AssertSeries(series, [70..90, 140..160, 210..230, 50..80, 150..180]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160, 210..230, 50..80, 150..180]);

        Assert.Equal(200, chunk.GetValues<byte>(eval, 10, new byte[200]));
        AssertSeries(series, [70..90, 140..160, 210..230, 50..80, 150..180, 10..210]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160, 210..230, 50..80, 150..180, 10..210]);
    }


    [Fact]
    public void RetrieveAvailableDataAtEnd()
    {
        var series = new MutableSeries<byte>("", 256);
        series.Expand(0, 256);
        var chunk = new LazySeries.Chunk(0, series);
        var eval = new Evaluator(128);

        Assert.Equal(20, chunk.GetValues<byte>(eval, 70, new byte[20]));
        AssertSeries(series, [70..90]);
        AssertSpan(eval.AccessTimes, 1, [70..90]);

        Assert.Equal(0, chunk.GetValues<byte>(eval, 140, new byte[20]));
        AssertSeries(series, [70..90, 140..160]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160]);

        Assert.Equal(0, chunk.GetValues<byte>(eval, 210, new byte[20]));
        AssertSeries(series, [70..90, 140..160, 210..230]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160, 210..230]);

        Assert.Equal(30, chunk.GetValues<byte>(eval, 50, new byte[30]));
        AssertSeries(series, [70..90, 140..160, 210..230, 50..80]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160, 210..230, 50..80]);

        Assert.Equal(0, chunk.GetValues<byte>(eval, 150, new byte[30]));
        AssertSeries(series, [70..90, 140..160, 210..230, 50..80, 150..180]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160, 210..230, 50..80, 150..180]);

        Assert.Equal(118, chunk.GetValues<byte>(eval, 10, new byte[200]));
        AssertSeries(series, [70..90, 140..160, 210..230, 50..80, 150..180, 10..210]);
        AssertSpan(eval.AccessTimes, 1, [70..90, 140..160, 210..230, 50..80, 150..180, 10..210]);
    }
}


file class Evaluator(int length) : ILazyEvaluator<byte>
{
    public const byte Value = 255;

    public ReadOnlySpan<byte> AccessTimes => _AccessTimes[..length];
    private readonly byte[] _AccessTimes = new byte[256];

    public int EvaluateValues(int startRowIndex, Span<byte> destination)
    {
        var evaluationLength = Math.Max(Math.Min(destination.Length, length - startRowIndex), 0);
        for(int i = 0; i < evaluationLength; i++)
        {
            ++_AccessTimes[startRowIndex + i];
        }
        destination.Fill(Value);
        return evaluationLength;
    }
}