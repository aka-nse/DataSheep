using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

public class FixedBitArray256Test
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
        var exp = (stackalloc char[256]);
        var act = (stackalloc char[256]);
        var bitArray = new FixedBitArray256();
        for(var i = 0; i < 256; ++i)
        {
            Assert.False(bitArray[i]);
        }
        foreach(var stage in flags)
        {
            for(var i = 0; i < 256; ++i)
            {
                bitArray[i] = stage[i];
            }

            for(var i = 0; i < 256; ++i)
            {
                exp[i] = stage[i] ? '1' : '0';
                act[i] = bitArray[i] ? '1' : '0';
            }
            var expected = exp.ToString();
            var actual = act.ToString();
            Assert.Equal(expected, actual);
        }
    }
}
