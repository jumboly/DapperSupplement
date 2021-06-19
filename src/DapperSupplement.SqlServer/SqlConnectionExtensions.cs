using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using DapperSupplement.SqlServer;
using FastMember;

// ReSharper disable once CheckNamespace
namespace Microsoft.Data.SqlClient
{
    public static class SqlConnectionExtensions
    {
        public static async Task<string> CreateTempTableAsync(this SqlConnection connection, string sourceTableName)
        {
            if (!connection.State.HasFlag(ConnectionState.Open))
            {
                throw new InvalidOperationException($"{nameof(connection)} がオープンされていません");
            }

            var tempTableName = $"#temp_{Guid.NewGuid():N}";
            await connection.ExecuteAsync($"select * into {tempTableName} from {sourceTableName} where 1=0")
                .ConfigureAwait(false);
            return tempTableName;
        }

        public static Task BulkCopyAsync<T>(
            this IDbConnection connection,
            string tableName,
            IEnumerable<T> records,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            IDbTransaction transaction = default,
            int? timeout = default,
            int? batchSize = default,
            SqlRowsCopiedEventHandler handler = default)
        {
            var sqlConnection = connection as SqlConnection;
            if (sqlConnection == null)
            {
                throw new ArgumentException(
                    $"{nameof(SqlConnection)} ではありません。:{connection.GetType().Name}", nameof(connection));
            }

            SqlTransaction sqlTransaction = transaction as SqlTransaction;
            if (transaction != null && sqlTransaction == null)
            {
                throw new ArgumentException(
                    $"{nameof(SqlTransaction)} ではありません。:{transaction.GetType().Name}", nameof(transaction));
            }

            return BulkCopyAsync(sqlConnection, tableName, records, options, sqlTransaction, timeout, batchSize, handler);
        }

        public static async Task BulkCopyAsync<T>(
            this SqlConnection connection,
            string tableName,
            IEnumerable<T> records,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            SqlTransaction transaction = default,
            int? timeout = default,
            int? batchSize = default,
            SqlRowsCopiedEventHandler handler = default)
        {
            if (!connection.State.HasFlag(ConnectionState.Open))
            {
                throw new InvalidOperationException($"{nameof(connection)} がオープンされていません");
            }
            
            using var bulkCopy =
                SqlBulkCopyBuilder.Create<T>(connection, tableName, options, transaction, timeout, batchSize, handler);
            
            await using var reader = ObjectReader.Create(records, TypeCache<T>.Members.ToArray());

            await bulkCopy.WriteToServerAsync(reader).ConfigureAwait(false);
        }

        public static Task<int> BulkUpdateAsync<T>(
            this IDbConnection connection,
            string tableName,
            IEnumerable<T> records,
            IDbTransaction transaction = default,
            int? timeout = default,
            int? batchSize = default)
        {
            var sqlConnection = connection as SqlConnection;
            if (sqlConnection == null)
            {
                throw new ArgumentException(
                    $"{nameof(SqlConnection)} ではありません。:{connection.GetType().Name}", nameof(connection));
            }

            var sqlTransaction = transaction as SqlTransaction;
            if (transaction != null && sqlTransaction == null)
            {
                throw new ArgumentException(
                    $"{nameof(SqlTransaction)} ではありません。:{transaction.GetType().Name}", nameof(transaction));
            }

            return BulkUpdateAsync(sqlConnection, tableName, records, sqlTransaction, timeout, batchSize);
        }

        public static async Task<int> BulkUpdateAsync<T>(
            this SqlConnection connection,
            string tableName,
            IEnumerable<T> records,
            SqlTransaction transaction = default,
            int? timeout = default,
            int? batchSize = default)
        {
            var tempTable = await CreateTempTableAsync(connection, tableName)
                .ConfigureAwait(false);
            await BulkCopyAsync(connection, tempTable, records, transaction: transaction, timeout: timeout, batchSize: batchSize)
                .ConfigureAwait(false);

            var keys = TypeCache<T>.KeyMembers;
            var contents = TypeCache<T>.ContentMembers;

            var setClause = string.Join(",", contents.Select(it => $"{it}=b.{it}"));
            var onClause = string.Join(" and ", keys.Select(it => $"a.{it}=b.{it}"));

            var sql = $"update {tableName} set {setClause} from {tableName} a inner join {tempTable} b on {onClause}";
            return await connection.ExecuteAsync(sql, transaction: transaction, commandTimeout: timeout)
                .ConfigureAwait(false);
        }

        public static Task<int> BulkMergeAsync<T>(
            this IDbConnection connection,
            string tableName,
            IEnumerable<T> records,
            IDbTransaction transaction = default,
            int? timeout = default,
            int? batchSize = default)
        {
            var sqlConnection = connection as SqlConnection;
            if (sqlConnection == null)
            {
                throw new ArgumentException(
                    $"{nameof(SqlConnection)} ではありません。:{connection.GetType().Name}", nameof(connection));
            }

            var sqlTransaction = transaction as SqlTransaction;
            if (transaction != null && sqlTransaction == null)
            {
                throw new ArgumentException(
                    $"{nameof(SqlTransaction)} ではありません。:{transaction.GetType().Name}", nameof(transaction));
            }

            return BulkMergeAsync(sqlConnection, tableName, records, sqlTransaction, timeout, batchSize);
        }
        
        public static async Task<int> BulkMergeAsync<T>(
            this SqlConnection connection,
            string tableName,
            IEnumerable<T> records,
            bool enableDelete = false,
            SqlTransaction transaction = default,
            int? timeout = default,
            int? batchSize = default)
        {
            var tempTable = await CreateTempTableAsync(connection, tableName)
                .ConfigureAwait(false);
            
            await BulkCopyAsync(connection, tempTable, records, transaction: transaction, timeout: timeout, batchSize: batchSize)
                .ConfigureAwait(false);

            var members = TypeCache<T>.Members;
            var keys = TypeCache<T>.KeyMembers;
            var contents = TypeCache<T>.ContentMembers;

            var onClause = string.Join(" and ", keys.Select(it => $"dst.{it}=src.{it}"));
            var setClause = string.Join(",", contents.Select(it => $"{it}=src.{it}"));
            var columnList = string.Join(",", members);
            var valueList = string.Join(",", members.Select(it => $"src.{it}"));

            var sql =
                $"merge {tableName} dst"
                + $" using {tempTable} src"
                + $" on {onClause}"
                + $" when matched then update set {setClause}"
                + $" when not matched by target then insert ({columnList}) values ({valueList})"
                + (enableDelete ? " when not matched by source then delete" : "")
                + ";";

            return await connection.ExecuteAsync(sql, transaction: transaction, commandTimeout: timeout)
                .ConfigureAwait(false);
        }
    }
}
