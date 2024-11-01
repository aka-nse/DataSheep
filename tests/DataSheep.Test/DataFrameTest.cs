using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

public class DataFrameTest
{
    [Fact]
    public void CreateDataRecord()
    {
        TestClassRecord[] records = [
            new(0, 0, "0"),
            new(1, 1, "1"),
            new(2, 2, "2"),
            new(3, 3, "3"),
            new(4, 4, "4"),
            new(5, 5, "5"),
        ];
        {
            // Create via ReadOnlySpan
            var df = DataFrame.Create((ReadOnlySpan<TestClassRecord>)records);
            Assert.IsType<DataFrame<TestClassRecord>.Immutable>(df);
            Assert.Equal(records.Length, df.RowCount);
            for(var j = 0; j < df.RowCount; ++j)
            {
                Assert.Equal(records[j], df[j]);
            }
            Assert.Equal(records, df.AsEnumerable());
        }
        {
            // Create via IReadOnlyList
            var df = DataFrame.Create((IReadOnlyList<TestClassRecord>)records);
            Assert.IsType<DataFrame<TestClassRecord>.Immutable>(df);
            Assert.Equal(records.Length, df.RowCount);
            for(var j = 0; j < df.RowCount; ++j)
            {
                Assert.Equal(records[j], df[j]);
            }
            Assert.Equal(records, df.AsEnumerable());
        }
        {
            // CreateMutable via ReadOnlySpan
            var df = DataFrame.CreateMutable((ReadOnlySpan<TestClassRecord>)records);
            Assert.IsType<DataFrame<TestClassRecord>.Mutable>(df);
            Assert.Equal(records.Length, df.RowCount);
            for(var j = 0; j < df.RowCount; ++j)
            {
                Assert.Equal(records[j], df[j]);
            }
            Assert.Equal(records, df.AsEnumerable());
        }
        {
            // CreateMutable via IReadOnlyList
            var df = DataFrame.CreateMutable((IReadOnlyList<TestClassRecord>)records);
            Assert.IsType<DataFrame<TestClassRecord>.Mutable>(df);
            Assert.Equal(records.Length, df.RowCount);
            for(var j = 0; j < df.RowCount; ++j)
            {
                Assert.Equal(records[j], df[j]);
            }
            Assert.Equal(records, df.AsEnumerable());
        }
    }
}
