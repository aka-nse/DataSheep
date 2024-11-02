// See https://aka.ms/new-console-template for more information
using DataSheep;

Console.WriteLine("Hello, World!");

[DataSheep.AutoDataRecord]
public partial record ClassRecord1(int X, int Y, string Z) : IDataRecord<ClassRecord1>;
