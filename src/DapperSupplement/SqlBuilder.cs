using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using FastMember;

namespace DapperSupplement
{
    public static class SqlBuilder
    {
        public static string CreateInsert(Type type, string tableName)
        {
            var accessor = TypeAccessor.Create(type);

            var columns = accessor.GetMembers()
                .OrderBy(it => it.Ordinal)
                .Select(it => it.Name)
                .ToArray();

            return $"insert into {tableName} ({string.Join(",", columns)}) values ({string.Join(",", columns.Select(it => $"@{it}"))})";
        }

        public static string CreateUpdate(Type type, string tableName)
        {
            var accessor = TypeAccessor.Create(type);
            
            var members = accessor.GetMembers()
                .OrderBy(it => it.Ordinal)
                .ToArray();

            var keyColumns = members
                .Where(it => it.GetAttribute(typeof(KeyAttribute), true) != null)
                .Select(it => it.Name);

            var setColumns = members
                .Where(it => it.GetAttribute(typeof(KeyAttribute), true) == null)
                .Select(it => it.Name);

            var setClause = string.Join(",", setColumns.Select(it => $"{it}=@{it}"));
            var whereClause = string.Join(" and ", keyColumns.Select(it => $"{it}=@{it}"));

            return $"update {tableName} set {setClause} where {whereClause}";
        }

        public static string CreateDelete(Type type, string tableName)
        {
            var accessor = TypeAccessor.Create(type);
            
            var members = accessor.GetMembers()
                .OrderBy(it => it.Ordinal)
                .ToArray();

            var keyColumns = members
                .Where(it => it.GetAttribute(typeof(KeyAttribute), true) != null)
                .Select(it => it.Name);

            var whereClause = string.Join(" and ", keyColumns.Select(it => $"{it}=@{it}"));

            return $"delete from {tableName} where {whereClause}";
        }
    }
}