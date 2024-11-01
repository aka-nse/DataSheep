using System.Collections;

namespace DataSheep;

public static partial class DataFrame
{
    internal static class OptionsHelper
    {
        public static IReadOnlyDictionary<int, string> MapColumnNamesFromList(IReadOnlyList<string> ColumnNames)
        {
            var dict = new Dictionary<int, string>();
            for(var i = 0; i < ColumnNames.Count; ++i)
            {
                dict[i] = ColumnNames[i];
            }
            return dict;
        }


        public static TemporaryBuffer<string> CreateTemporaryColumnNames(IRecordTrait trait, IReadOnlyDictionary<int, string> columnNameMap, out IReadOnlyList<string> builtColumnNames)
        {
            var buffer = new TemporaryBuffer<string>(trait.ColumnCount);
            for(var i = 0; i < trait.ColumnCount; ++i)
            {
                buffer.Span[i] = trait.DefaultColumnNames[i];
            }
            foreach(var kv in columnNameMap)
            {
                buffer.Span[kv.Key] = kv.Value;
            }
            builtColumnNames = new ReadOnlyListSegment<string>(buffer.Array, 0, trait.ColumnCount);
            return buffer;
        }
    }

    private class ColumnNameList(IReadOnlyDictionary<int, string> nameMap, IReadOnlyList<string> defaultNames) : IReadOnlyList<string>
    {
        public int Count => defaultNames.Count;

        public string this[int index]
            => nameMap.TryGetValue(index, out var name)
                ? name
            : defaultNames[index];

        public IEnumerator<string> GetEnumerator()
        {
            for(var i = 0; i < Count; ++i)
            {
                yield return this[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}
