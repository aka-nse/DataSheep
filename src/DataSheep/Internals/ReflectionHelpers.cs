using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSheep;

internal static class ReflectionHelpers
{
    /// <summary>
    /// Determines <paramref name="type"/> is closed generic type of <paramref name="genericTypeDefinition"/> or not.
    /// </summary>
    /// <param name="type"></param>
    /// <param name="genericTypeDefinition"></param>
    /// <returns></returns>
    public static bool IsClosedGenericOf(Type type, Type genericTypeDefinition)
        => type.IsGenericType && type.GetGenericTypeDefinition() == genericTypeDefinition;

}
