using System.Diagnostics;

// ReSharper disable once CheckNamespace
namespace System.Collections.Generic
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<IReadOnlyList<T>> TakeBy<T>(this IEnumerable<T> enumerable, int count)
        {
            var list = new List<T>(count);
            foreach (var item in enumerable)
            {
                list.Add(item);
                if (list.Count == count)
                {
                    yield return list.ToArray();
                    list.Clear();
                }
                Debug.Assert(list.Count < count);
            }

            if (list.Count > 0)
            {
                yield return list.ToArray();
            }
        }  
    }
}