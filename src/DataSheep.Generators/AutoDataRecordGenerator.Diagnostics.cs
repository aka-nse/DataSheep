using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.CodeAnalysis;

namespace DataSheep.Generators;

partial class AutoDataRecordGenerator
{
    private static class Diagnostics
    {
        public static readonly DiagnosticDescriptor MustBeRecord
            = new (
                id: "DS1001",
                title: "Must be record",
                messageFormat: "Data record type to be implemented automatically must be a record type.",
                category: "Implementation",
                DiagnosticSeverity.Error,
                true);

        public static readonly DiagnosticDescriptor MustImplementIDataRecord
            = new (
                id: "DS1002",
                title: "Must be record",
                messageFormat: "Data record type to be implemented automatically must implement IDataRecord<TSelf>.",
                category: "Implementation",
                DiagnosticSeverity.Error,
                true);

        public static readonly DiagnosticDescriptor MustHavePrimaryConstructor
            = new (
                id: "DS1003",
                title: "Must have primary constructor",
                messageFormat: "Data record type to be implemented automatically must have primary constructor.",
                category: "Implementation",
                DiagnosticSeverity.Error,
                true);
    }
}
