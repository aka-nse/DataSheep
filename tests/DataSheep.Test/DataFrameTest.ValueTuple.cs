using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

public partial class DataFrameTest
{
    [Fact]
    public void CreateValueTuple1()
    {
        var dataFrame = DataFrame.Create<(int x, int y)>();
        Assert.Equal("Column 1", dataFrame.ColumnNames[0]);
        Assert.Equal("Column 2", dataFrame.ColumnNames[1]);
    }

    [Fact]
    public void CreateValueTuple2()
    {
        var dataFrame = DataFrame.Create<(int x, int y)>(columnNames: ["x", "y"]);
        Assert.Equal("x", dataFrame.ColumnNames[0]);
        Assert.Equal("y", dataFrame.ColumnNames[1]);
    }

    [Fact]
    public void CreateValueTuple3()
    {
        var dataFrame = DataFrame.Create<(int x1, int x2, int x3, int x4, int x5, int x6, int x7, int x8, int x9, int x10, int x11, int x12, int x13, int x14, int x15, int x16)>(columnNames: ["x", "y"]);
        Assert.Equal("x", dataFrame.ColumnNames[0]);
        Assert.Equal("y", dataFrame.ColumnNames[1]);
        Assert.Equal("Column 3", dataFrame.ColumnNames[2]);
        Assert.Equal("Column 4", dataFrame.ColumnNames[3]);
        Assert.Equal("Column 5", dataFrame.ColumnNames[4]);
        Assert.Equal("Column 6", dataFrame.ColumnNames[5]);
        Assert.Equal("Column 7", dataFrame.ColumnNames[6]);
        Assert.Equal("Column 8", dataFrame.ColumnNames[7]);
        Assert.Equal("Column 9", dataFrame.ColumnNames[8]);
        Assert.Equal("Column 10", dataFrame.ColumnNames[9]);
        Assert.Equal("Column 11", dataFrame.ColumnNames[10]);
        Assert.Equal("Column 12", dataFrame.ColumnNames[11]);
        Assert.Equal("Column 13", dataFrame.ColumnNames[12]);
        Assert.Equal("Column 14", dataFrame.ColumnNames[13]);
        Assert.Equal("Column 15", dataFrame.ColumnNames[14]);
        Assert.Equal("Column 16", dataFrame.ColumnNames[15]);
    }

}
