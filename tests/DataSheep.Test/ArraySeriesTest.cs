using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

public class ArraySeriesTest
{
    public static TheoryData<int, int> InitializeTestCase()
        => new()
        {
            { 1, 256 },
            { 2, 256 },
            { 3, 256 },
            { 4, 256 },
            { 16, 256 },
            { 256, 256 },
            { 257, 512 },
            { 512, 512 },
        };
    [Theory]
    [MemberData(nameof(InitializeTestCase))]
    public void Initialize(int capacityRequest, int expectedInitialCapacity)
    {
        var series = new ArraySeries<int>("", capacityRequest);
        Assert.Equal(expectedInitialCapacity, series.Capacity);
    }

    [Fact]
    public void Expend()
    {
        var series = new ArraySeries<int>("", 256);
        series.Expand(0, 256);
        Assert.Throws<ArgumentOutOfRangeException>(() => series.Expand(-1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => series.Expand(0, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => series.Expand(257, 1));
        series.Expand(256, 1);
        series.Expand(128, 1);
    }

    [Fact]
    public void Shrink()
    {
        var series = new ArraySeries<int>("", 256);
        series.Expand(0, 256);
        Assert.Throws<ArgumentOutOfRangeException>(() => series.Shrink(-1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => series.Shrink(0, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => series.Shrink(256, 1));
        series.Shrink(255, 1);
        series.Shrink(128, 1);
    }

    [Fact]
    public void Add()
    {
        var series = new ArraySeries<int>("", 256);
        for(var i = 0; i < 1024; ++i)
        {
            series.Add(i);
        }
        for(var i = 0; i < 1024; ++i)
        {
            Assert.Equal(i, series[i]);
        }
        Assert.Throws<IndexOutOfRangeException>(() => series[1024]);
    }

    [Fact]
    public void AddRange()
    {
        var series = new ArraySeries<int>("", 256);
        for(var i = 0; i < 1024; i += 256)
        {
            var data = Enumerable.Range(i, 256).ToArray();
            series.AddRange(data);
        }
        for(var i = 0; i < 1024; ++i)
        {
            Assert.Equal(i, series[i]);
        }
        Assert.Throws<IndexOutOfRangeException>(() => series[1024]);
    }

    [Fact]
    public void Insert()
    {
        var list = new List<int>();
        var series = new ArraySeries<int>("", 256);

        var data = Enumerable.Range(0, 256).ToArray();
        list.AddRange(data);
        series.AddRange(data);
        
        for(var i = 0; i < 1024; ++i)
        {
            list.Insert(128, i);
            series.Insert(128, i);
        }
        for(var i = 0; i < list.Count; ++i)
        {
            Assert.Equal(list[i], series[i]);
        }
        Assert.Throws<IndexOutOfRangeException>(() => series[list.Count]);
    }

    [Fact]
    public void RemoveAt()
    {
        var list = new List<int>();
        var series = new ArraySeries<int>("", 1024);

        var data = Enumerable.Range(0, 1024).ToArray();
        list.AddRange(data);
        series.AddRange(data);

        for(var i = 0; i < 256; ++i)
        {
            list.RemoveAt(i);
            series.RemoveAt(i);
        }
        for(var i = 0; i < list.Count; ++i)
        {
            Assert.Equal(list[i], series[i]);
        }
        Assert.Throws<IndexOutOfRangeException>(() => series[list.Count]);
    }

    [Fact]
    public void Clear()
    {
        var refs = new List<WeakReference<object>>();
        var series = new ArraySeries<object>("", 1024);

        static void innerScope(List<WeakReference<object>> refs, ArraySeries<object> series)
        {
            for(var i = 0; i < 1024; ++i)
            {
                var obj = new object();
                refs.Add(new WeakReference<object>(obj));
                series.Add(obj);
            }
            Assert.Equal(1024, series.Count);
            series.Clear();
        }

        innerScope(refs, series);
        Assert.Equal(0, series.Count);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        Assert.True(refs.All(wref => !wref.TryGetTarget(out _)));
    }
}
