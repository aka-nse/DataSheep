using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace DataSheep.Generators;
using static InternalHelpers;

[Generator(LanguageNames.CSharp)]
public partial class AutoDataRecordGenerator : IIncrementalGenerator
{
    private const string NamespaceName = nameof(DataSheep);
    private const string ClassName = "AutoDataRecordAttribute";
    private const string DataRecordInterfaceName = "IDataRecord";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        context.RegisterPostInitializationOutput(static context =>
        {
            context.AddSource($"{NamespaceName}.{ClassName}.g.cs", $$"""
                namespace {{NamespaceName}};
                using System;

                [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
                internal sealed class {{ClassName}} : Attribute;
                """);
        });
        var source = context.SyntaxProvider.ForAttributeWithMetadataName(
            $"{NamespaceName}.{ClassName}",
            static (node, token) => true,
            static (context, token) => context);
        context.RegisterSourceOutput(source, Emit);
    }


    private static void Emit(SourceProductionContext context, GeneratorAttributeSyntaxContext source)
    {
        var node = (TypeDeclarationSyntax)source.TargetNode;
        var symbol = (INamedTypeSymbol)source.TargetSymbol;
        var semanticModel = source.SemanticModel;
        if(node is not RecordDeclarationSyntax recordDeclNode)
        {
            context.ReportDiagnostic(Diagnostic.Create(Diagnostics.MustBeRecord, node.Identifier.GetLocation()));
            return;
        }

        bool isIDataRecordImplementation(INamedTypeSymbol ifType)
        {
            if(!ifType.IsGenericType)
            {
                return false;
            }
            if(ifType.TypeArguments.Length != 1)
            {
                return false;
            }
            if(!SymbolEqualityComparer.Default.Equals(ifType.TypeArguments[0], symbol))
            {
                return false;
            }
            return ifType.ConstructUnboundGenericType().ToDisplayString() == $"{NamespaceName}.{DataRecordInterfaceName}<>";
        }

        if(symbol.Interfaces.Where(isIDataRecordImplementation).Count() != 1)
        {
            context.ReportDiagnostic(Diagnostic.Create(Diagnostics.MustImplementIDataRecord, node.Identifier.GetLocation()));
            return;
        }

        if(node.ChildNodes().SingleOrDefault(static node => node is ParameterListSyntax) is not ParameterListSyntax parameterListNode)
        {
            context.ReportDiagnostic(Diagnostic.Create(Diagnostics.MustHavePrimaryConstructor, node.Identifier.GetLocation()));
            return;
        }

        var parameters = parameterListNode
            .Parameters
            .Select(p => (IParameterSymbol)semanticModel.GetDeclaredSymbol(p, context.CancellationToken)!)
            .ToArray();

        var generatedNamespace = !symbol.ContainingNamespace.IsGlobalNamespace ? $"""
                namespace {symbol.ContainingNamespace.ToDisplayString()};

                """ : "";
        var sourceCode = $$"""
                {{generatedNamespace}}
                partial record {{(symbol.IsValueType ? "struct": "class")}} {{symbol.Name}}
                {
                    public static {{NamespaceName}}.IRecordTrait<{{symbol.Name}}> Trait { get; } = new RecordTrait();
                }

                file sealed class RecordTrait : {{NamespaceName}}.IRecordTrait<{{symbol.Name}}>
                {
                    /// <inheritdoc />
                    public IReadOnlyList<string> DefaultColumnNames { get; }
                        = [
                            {{parameters.LineJoined(3, (p, i) => $"\"{p.Name}\",")}}
                        ];

                    /// <inheritdoc />
                    public {{NamespaceName}}.IMutableSeries CreateSeries(
                        int columnIndex,
                        int initialCapacity,
                        string columnName)
                        => columnIndex switch {
                            {{parameters.LineJoined(3, (p, i) => $"{i} => new {NamespaceName}.MutableSeries<{p.Type.ToDisplayString()}>(columnName, initialCapacity),")}}
                            _ => throw new ArgumentOutOfRangeException(nameof(columnIndex)),
                        };
            
                    /// <inheritdoc />
                    public void ReadFromSeries(
                        System.ReadOnlySpan<{{NamespaceName}}.ISeries> series,
                        int rowIndex,
                        System.Span<{{symbol.Name}}> destination)
                    {
                        {{parameters.LineJoined(2, (p, i) => $"var ser{i} = series[{i}];")}}
                        for(var j = 0; j < destination.Length; ++j)
                        {
                            var row = rowIndex + j;
                            destination[j] = new ({{parameters.CommaJoined((p, i) => $"ser{i}.GetValue<{p.Type.ToDisplayString()}>(row)")}});
                        }
                    }
            
                    /// <inheritdoc />
                    public void WriteToSeries(
                        System.ReadOnlySpan<{{NamespaceName}}.IMutableSeries> series,
                        int rowIndex,
                        System.ReadOnlySpan<{{symbol.Name}}> source)
                    {
                        {{parameters.LineJoined(2, (p, i) => $"var ser{i} = series[{i}];")}}
                        for(var j = 0; j < source.Length; ++j)
                        {
                            var row = rowIndex + j;
                            var ({{CommaJoined(0, parameters.Length, i => $"x{i}")}}) = source[j];
                            {{parameters.LineJoined(3, (p, i) => $"ser{i}.SetValue<{p.Type.ToDisplayString()}>(row, x{i});")}}
                        }
                    }
                }
                """;
        context.AddSource($"{symbol.Name}.Trait.g.cs", sourceCode);
    }
}
