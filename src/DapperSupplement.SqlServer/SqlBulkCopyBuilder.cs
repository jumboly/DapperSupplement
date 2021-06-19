using System.Collections.Generic;
using System.Linq;
using FastMember;
using Microsoft.Data.SqlClient;

namespace DapperSupplement.SqlServer
{
    public static class SqlBulkCopyBuilder
    {
        public static SqlBulkCopy Create<T>(
            SqlConnection connection,
            string tableName,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            SqlTransaction transaction = default,
            int? timeout = default,
            int? batchSize = default,
            SqlRowsCopiedEventHandler handler = default)
        {
            var bulkCopy = new SqlBulkCopy(connection, options, transaction)
            {
                DestinationTableName = tableName,
            };

            if (timeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = timeout.Value;
            }
            if (batchSize.HasValue)
            {
                bulkCopy.BatchSize = batchSize.Value;
            }
            if (handler != default)
            {
                bulkCopy.SqlRowsCopied += handler;
            }

            foreach (var member in TypeCache<T>.Members)
            {
                bulkCopy.ColumnMappings.Add(member, member);
            }

            return bulkCopy;
        }
    }
}