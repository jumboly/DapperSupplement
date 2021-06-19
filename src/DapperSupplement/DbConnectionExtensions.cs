using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using DapperSupplement;

// ReSharper disable once CheckNamespace
namespace System.Data
{
    public static class DbConnectionExtensions
    {
        public static Task<int> InsertAsync<T>(this IDbConnection connection, string tableName, T record)
        {
            var sql = GetInsert<T>(tableName);
            return connection.ExecuteAsync(sql, record);
        }

        public static async Task<int> BatchInsertAsync<T>(this IDbConnection connection, string tableName, IEnumerable<T> records, int? batchSize = default)
        {
            var sql = GetInsert<T>(tableName);

            int count = 0;
            foreach (var recordGroup in records.TakeBy(batchSize ?? 100))
            {
                count += await connection.ExecuteAsync(sql, recordGroup)
                    .ConfigureAwait(false);
            }

            return count;
        }

        public static Task<int> UpdateAsync<T>(this IDbConnection connection, string tableName, T record)
        {
            var sql = GetUpdate<T>(tableName);
            return connection.ExecuteAsync(sql, record);
        }
        
        public static async Task<int> BatchUpdateAsync<T>(this IDbConnection connection, string tableName, IEnumerable<T> records, int? batchSize = default)
        {
            var sql = GetUpdate<T>(tableName);

            int count = 0;
            foreach (var recordGroup in records.TakeBy(batchSize ?? 100))
            {
                count += await connection.ExecuteAsync(sql, recordGroup)
                    .ConfigureAwait(false);
            }

            return count;
        }
        
        public static Task<int> DeleteAsync<T>(this IDbConnection connection, string tableName, T record)
        {
            var sql = GetDelete<T>(tableName);
            return connection.ExecuteAsync(sql, record);
        }
        
        public static async Task<int> BatchDeleteAsync<T>(this IDbConnection connection, string tableName, IEnumerable<T> records, int? batchSize = default)
        {
            var sql = GetDelete<T>(tableName);

            int count = 0;
            foreach (var recordGroup in records.TakeBy(batchSize ?? 100))
            {
                count += await connection.ExecuteAsync(sql, recordGroup)
                    .ConfigureAwait(false);
            }

            return count;
        }

        private static readonly ConcurrentDictionary<(Type, string), string> InsertCache = new ();
        private static string GetInsert<T>(string tableName)
        {
            return InsertCache.GetOrAdd((typeof(T), tableName), it => 
                SqlBuilder.CreateInsert(it.Item1, it.Item2));
        }

        private static readonly ConcurrentDictionary<(Type, string), string> UpdateCache = new();
        private static string GetUpdate<T>(string tableName)
        {
            return UpdateCache.GetOrAdd((typeof(T), tableName), it => 
                SqlBuilder.CreateUpdate(it.Item1, it.Item2));
        }
        
        private static readonly ConcurrentDictionary<(Type, string), string> DeleteCache = new();
        private static string GetDelete<T>(string tableName)
        {
            return UpdateCache.GetOrAdd((typeof(T), tableName), it => 
                SqlBuilder.CreateDelete(it.Item1, it.Item2));
        }
    }
}