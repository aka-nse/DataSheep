using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

public class RecordTraitTest
{
    [Fact]
    public void ForDataRecord()
    {
        Assert.True(RecordTrait.Cache<TestClassRecord>.Trait is TestClassRecord.TraitImpl);
        Assert.True(RecordTrait.Cache<TestStructRecord>.Trait is TestStructRecord.TraitImpl);
    }

    [Fact]
    public void ForValueTuple()
    {
        Assert.True(RecordTrait.Cache<(int, int)>.Trait is ValueTupleTrait<int, int>);
        Assert.True(RecordTrait.Cache<(int, uint)>.Trait is ValueTupleTrait<int, uint>);
        Assert.True(RecordTrait.Cache<(uint, int)>.Trait is ValueTupleTrait<uint, int>);
        Assert.True(RecordTrait.Cache<(int, int, int)>.Trait is ValueTupleTrait<int, int, int>);
        Assert.True(RecordTrait.Cache<(
                int x01, int x02, int x03, int x04, int x05, int x06, int x07,
                int x08, int x09, int x10, int x11, int x12, int x13, int x14,
                int x15, int x16, int x17, int x18, int x19, int x20)>.Trait
            is ValueTupleTrait<
                int/*x01*/, int/*x02*/, int/*x03*/, int/*x04*/, int/*x05*/, int/*x06*/, int/*x07*/,
                (int x08, int x09, int x10, int x11, int x12, int x13, int x14, int x15, int x16, int x17, int x18, int x19, int x20),
                ValueTupleTrait<
                    int/*x08*/, int/*x09*/, int/*x10*/, int/*x11*/, int/*x12*/, int/*x13*/, int/*x14*/,
                    (int x15, int x16, int x17, int x18, int x19, int x20),
                    ValueTupleTrait<int/*x15*/, int/*x16*/, int/*x17*/, int/*x18*/, int/*x19*/, int/*x20*/>>>);

#pragma warning disable CS0184
        Assert.False(RecordTrait.Cache<(int, int)>.Trait is ValueTupleTrait<int, uint>);
        Assert.False(RecordTrait.Cache<(int, int)>.Trait is ValueTupleTrait<uint, int>);
#pragma warning restore CS0184
    }

    [Fact]
    public void ForFallback()
    {
        Assert.True(RecordTrait.Cache<object>.Trait is FallbackRecordTrait<object>);
        Assert.True(RecordTrait.Cache<int>.Trait is FallbackRecordTrait<int>);
    }
}
