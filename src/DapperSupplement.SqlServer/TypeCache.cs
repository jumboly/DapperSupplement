using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using FastMember;

namespace DapperSupplement.SqlServer
{
    internal static class TypeCache<T>
    {
        public static IReadOnlyList<string> Members { get; } =
            TypeAccessor.Create(typeof(T)).GetMembers()
                .OrderBy(it => it.Ordinal)
                .Select(it => it.Name)
                .ToArray();
        
        public static IReadOnlyList<string> KeyMembers { get; } =
            TypeAccessor.Create(typeof(T)).GetMembers()
                .OrderBy(it => it.Ordinal)
                .Where(it => it.GetAttribute(typeof(KeyAttribute), true) != null)
                .Select(it => it.Name)
                .ToArray();
            
        public static IReadOnlyList<string> ContentMembers { get; } =
            TypeAccessor.Create(typeof(T)).GetMembers()
                .OrderBy(it => it.Ordinal)
                .Where(it => it.GetAttribute(typeof(KeyAttribute), true) == null)
                .Select(it => it.Name)
                .ToArray();
    }
}