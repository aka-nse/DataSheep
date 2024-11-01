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

        var sb = new StringBuilder();
        if(!symbol.ContainingNamespace.IsGlobalNamespace)
        {
            sb.AppendLine($"""
                namespace {symbol.ContainingNamespace.Name}";
                """);
        }
        sb.AppendLine($$"""
                partial class {{symbol.Name}}
                {
                    public static {{NamespaceName}}.IRecordTrait<{{symbol.Name}}> Trait { get; } = new RecordTrait();
                }

                file sealed class RecordTrait : {{NamespaceName}}.IRecordTrait<{{symbol.Name}}>
                {
                    /// <inheritdoc />
                    public {{NamespaceName}}.IMutableSeries[] CreateSeriesPrefab(int initialCapacity, System.Collections.Generic.IReadOnlyList<string> columnNames)
                        => new {{NamespaceName}}.IMutableSeries[]{
                """);
        foreach(var (p, i) in parameters.Select((p, i) => (p, i)))
        {
            sb.AppendLine($$"""
                            new {{NamespaceName}}.ArraySeries<{{p.Type.ToDisplayString()}}>>(columnNames[{{i}}], initialCapacity),
                """);
        }
        sb.AppendLine($$"""
                        };
            
                    /// <inheritdoc />
                    public void ReadFromSeries(System.ReadOnlySpan<{{NamespaceName}}.ISeries> series, int rowIndex, System.Span<{{symbol.Name}}> destination)
                    {
                """);
        foreach(var (p, i) in parameters.Select((p, i) => (p, i)))
        {
            sb.AppendLine($$"""
                        var x{{i}} = series[{{i}}].As<{{p.Type.ToDisplayString()}}>();
                """);
        }
        sb.AppendLine($$"""
                        for(var j = 0; j < destination.Length; ++j)
                        {
                            var row = rowIndex + j;
                            destination[j] = new ({{CommaJoined(1, parameters.Length, i => $"x{i}[row]")}});
                        }
                    }
            
                    /// <inheritdoc />
                    public void WriteToSeries(System.ReadOnlySpan<{{NamespaceName}}.IMutableSeries> series, int rowIndex, System.ReadOnlySpan<{{symbol.Name}}> source)
                    {
                """);
        foreach(var (p, i) in parameters.Select((p, i) => (p, i)))
        {
            sb.AppendLine($$"""
                        var x{{i}} = series[{{i}}].As<{{p.Type.ToDisplayString()}}>();
                """);
        }
        sb.AppendLine($$"""
                        for(var j = 0; j < source.Length; ++j)
                        {
                            var row = rowIndex + j;
                            ({{CommaJoined(1, parameters.Length, i => $"x{i}[row]")}}) = source[j];
                        }
                    }
                }
                """);
        context.AddSource($"{symbol.Name}.Trait.g.cs", sb.ToString());
    }
}
