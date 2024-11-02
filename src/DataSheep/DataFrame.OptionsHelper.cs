using System.Collections;

namespace DataSheep;

public static partial class DataFrame
{
    internal static class OptionsHelper
    {
        public static IReadOnlyDictionary<int, string> MapColumnNamesFromList(
            IReadOnlyList<string> ColumnNames)
        {
            var dict = new Dictionary<int, string>();
            for(var i = 0; i < ColumnNames.Count; ++i)
            {
                dict[i] = ColumnNames[i];
            }
            return dict;
        }
    }

    private class ColumnNameList(
        IReadOnlyDictionary<int, string> nameMap,
        IReadOnlyList<string> defaultNames)
        : IReadOnlyList<string>
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
