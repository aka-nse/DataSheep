using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep.Generators.Sample;

[DataSheep.AutoDataRecord]
public partial record struct ClassRecord2(string X, string Y, Entity Z) : IDataRecord<ClassRecord2>;

public record Entity;
