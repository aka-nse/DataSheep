using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

public partial class FixedBitArray256Test
{
    public static TheoryData<bool[][]> SetGetTestCase()
    {
        static bool[] createData(Func<int, bool> predicate)
            => Enumerable.Range(0, 256).Select(predicate).ToArray();

        var data = new TheoryData<bool[][]>
        {
            ([
                createData(i => i % 2 == 0),
            ]),
            ([
                createData(i => false),
                createData(i => true),
                createData(i => i % 2 == 0),
                createData(i => i % 3 == 0),
                createData(i => i % 7 == 0),
                createData(i => i % 11 == 0),
                createData(i => i < 128),
                .. Enumerable.Range(0, 256).Select(j => createData(i => i == j)),
            ]),
        };
        return data;
    }

    [Theory]
    [MemberData(nameof(SetGetTestCase))]
    public void SetGet(bool[][] flags)
    {
        var expected = new FixedBitArray256Reference();
        var actual = new FixedBitArray256();
        for(var i = 0; i < 256; ++i)
        {
            Assert.False(actual[i]);
        }
        foreach(var stage in flags)
        {
            for(var i = 0; i < 256; ++i)
            {
                expected[i] = stage[i];
                actual[i] = stage[i];
            }

            Assert.Equal(expected.ToString(), actual.ToString());
        }
    }


    public static TheoryData<bool[], int, int> AllTestCase()
    {
        static bool[] createData(Func<int, bool> predicate)
            => Enumerable.Range(0, 256).Select(predicate).ToArray();

        var data = new TheoryData<bool[], int, int>
        {
            { createData(i => i % 2 == 0), 0, 0 },
            { createData(i => i % 2 == 0), 0, 256 },
            { createData(i => i % 2 == 0), 0, 1 },
            { createData(i => i % 2 == 0), 1, 2 },
            { createData(i => i % 16 < 8), 0, 4 },
            { createData(i => i % 16 < 8), 4, 8 },
            { createData(i => i % 16 < 8), 8, 12 },
            { createData(i => i < 128), 32, 64 },
            { createData(i => i < 128), 96, 128 },
        };
        return data;
    }

    [Theory]
    [MemberData(nameof(AllTestCase))]
    public void All(bool[] flags, int start, int end)
    {
        var expected = new FixedBitArray256Reference();
        var actual = new FixedBitArray256();
        for(var i = 0; i < 256; ++i)
        {
            expected[i] = flags[i];
            actual[i] = flags[i];
        }

        Assert.Equal(expected.All(start..end), actual.All(start..end));
    }


    public static TheoryData<bool[]> GetNextFalseChunkTestData()
    {
        var data = new TheoryData<bool[]>();
        void add(Range[] chunks)
        {
            var flags = Enumerable.Range(0, 256).Select(_ => true).ToArray();
            foreach(var range in chunks)
            {
                var rangeStart = range.Start.Value;
                var rangeEnd = range.End.Value;
                for(var i = rangeStart; i < rangeEnd; ++i)
                {
                    flags[i] = false;
                }
            }

            var expected = new List<Range>();
            var ystart = -1;
            for(var i = 0; i < 256; ++i)
            {
                if(ystart < 0)
                {
                    if(!flags[i])
                    {
                        ystart = i;
                    }
                    continue;
                }
                else
                {
                    if(flags[i])
                    {
                        expected.Add(new(ystart, i - ystart));
                    }
                    continue;
                }
            }
            if(ystart >= 0)
            {
                expected.Add(new(ystart, 256 - ystart));
            }
            data.Add(flags);
        }

        add([]);
        add([new(16, 32)]);
        add([new(64, 128)]);
        add([new(0, 16), new(64, 80), new(128, 144), new(192, 208)]);
        add([.. Enumerable.Range(0, 16).Select(x => new Range(x * 16 + 4, x * 16 + 12))]);
        return data;
    }

    [Theory]
    [MemberData(nameof(GetNextFalseChunkTestData))]
    public void GetNextFalseChunk(bool[] flags)
    {
        var expected = new FixedBitArray256Reference();
        var actual = new FixedBitArray256();
        for(var i = 0; i < 256; ++i)
        {
            expected[i] = flags[i];
            actual[i] = flags[i];
        }

        var x = 0;
        var y = 0;
        while(x < 256 && y < 256)
        {
            Assert.Equal(expected.GetNextFalseChunk(ref x), actual.GetNextFalseChunk(ref y));
            Assert.Equal(x, y);
        }
    }

    public static TheoryData<Range[]> BulkSetTestCase()
    {
        var data = new TheoryData<Range[]>
        {
            { [0..256,] },
            { [0..256, 0..128, 0..64, 0..32, 0..16, 0..8, 0..4, 0..2, 0..1,] },
            { [0..255, 0..127, 0..63, 0..31, 0..15, 0..7, 0..3, 0..1,] },
            { [0..255, 64..192, 32..224, 96..160,] },
        };

        // random test cases
        var random = new Random(123456789);
        for(var i = 0; i < 20; ++i)
        {
            var ranges = new Range[random.Next(1, 100)];
            for(var j = 0; j < ranges.Length; ++j)
            {
                var start = random.Next(0, 256);
                var end = random.Next(start, 256);
                ranges[j] = start..end;
            }
            data.Add(ranges);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(BulkSetTestCase))]
    public void BulkSet(Range[] ranges)
    {
        var expected = new FixedBitArray256Reference();
        var actual = new FixedBitArray256();
        for(var i = 0; i < ranges.Length; ++i)
        {
            expected.BulkSet(ranges[i], i % 2 == 0);
            actual.BulkSet(ranges[i], i % 2 == 0);
            Assert.Equal(expected.ToString(), actual.ToString());
        }
    }



}
