using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DapperSupplement.SqlServer.Test.Tests
{
    public class SqlConnectionExtensionsTest
    {
        private const string Skip = "ローカルでのみ実行";
        // private const string Skip = null;
        
        private static readonly string ConnectionString = new SqlConnectionStringBuilder
        {
            DataSource = "localhost",
            InitialCatalog = "dapper_supplement",
            UserID = "SA",
            Password = "p@ssw0rd"
        }.ConnectionString;
        
        [Fact(Skip = Skip)]
        public async Task BulkCopy()
        {
            await using var connection = await OpenAsync();
            var fooTable = await CreateFooTableAsync(connection);

            var foos = new[]
            {
                new Foo() {Id = 1, Name = "a"},
                new Foo() {Id = 2, Name = "b"},
                new Foo() {Id = 3, Name = "c"},
            };
            await connection.BulkCopyAsync(fooTable, foos);

            var foos2 = (await connection.QueryAsync<Foo>($"select * from {fooTable} order by Id")).ToArray();
            
            Assert.Equal(foos, foos2);
        }

        [Fact(Skip = Skip)]
        public async Task BulkUpdate()
        {
            await using var connection = await OpenAsync();
            var fooTable = await CreateFooTableAsync(connection);

            var foos = new[]
            {
                new Foo() {Id = 1, Name = "a"},
                new Foo() {Id = 2, Name = "b"},
                new Foo() {Id = 3, Name = "c"},
            };
            await connection.BulkCopyAsync(fooTable, foos);

            foos[0].Name = "d";
            foos[1].Id = 99; foos[1].Name = "e"; // キーを変えたので一致しない
            foos[2].Name = "f";

            await connection.BulkUpdateAsync(fooTable, foos);

            var actual = (await connection.QueryAsync<Foo>($"select * from {fooTable} order by Id")).ToArray();
            var expected = new[]
            {
                new Foo() {Id = 1, Name = "d"},
                new Foo() {Id = 2, Name = "b"}, // 変化なし
                new Foo() {Id = 3, Name = "f"},
            };
            
            Assert.Equal(expected, actual);
        }

        [Fact(Skip = Skip)]
        public async Task BulkMerge()
        {
            await using var connection = await OpenAsync();
            var fooTable = await CreateFooTableAsync(connection);

            var foos = new[]
            {
                new Foo() {Id = 1, Name = "a"},
                new Foo() {Id = 2, Name = "b"},
                new Foo() {Id = 3, Name = "c"},
            };
            await connection.BulkCopyAsync(fooTable, foos);

            var foos2 = new[]
            {
                new Foo() {Id = 2, Name = "b2"},
                new Foo() {Id = 4, Name = "d"},
            };
            await connection.BulkMergeAsync(fooTable, foos2);

            var actual = (await connection.QueryAsync<Foo>($"select * from {fooTable} order by Id")).ToArray();
            var expected = new[]
            {
                new Foo() {Id = 1, Name = "a"},
                new Foo() {Id = 2, Name = "b2"},
                new Foo() {Id = 3, Name = "c"},
                new Foo() {Id = 4, Name = "d"},
            };
            
            Assert.Equal(expected, actual);
        }

        [Fact(Skip = Skip)]
        public async Task BulkMerge_EnableDelete()
        {
            await using var connection = await OpenAsync();
            var fooTable = await CreateFooTableAsync(connection);

            var foos = new[]
            {
                new Foo() {Id = 1, Name = "a"},
                new Foo() {Id = 2, Name = "b"},
                new Foo() {Id = 3, Name = "c"},
            };
            await connection.BulkCopyAsync(fooTable, foos);

            var foos2 = new[]
            {
                new Foo() {Id = 2, Name = "b2"},
                new Foo() {Id = 4, Name = "d"},
            };
            await connection.BulkMergeAsync(fooTable, foos2, enableDelete: true);

            var actual = (await connection.QueryAsync<Foo>($"select * from {fooTable} order by Id")).ToArray();
            var expected = new[]
            {
                new Foo() {Id = 2, Name = "b2"},
                new Foo() {Id = 4, Name = "d"},
            };
            
            Assert.Equal(expected, actual);
        }

        private async Task<SqlConnection> OpenAsync()
        {
            var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync().ConfigureAwait(false);
            return connection;
        }

        private async Task<string> CreateFooTableAsync(SqlConnection connection)
        {
            var tableName = $"#foo_{Guid.NewGuid():N}";
            var sql = $"create table {tableName} (Id int primary key, Name nvarchar(100))";
            await connection.ExecuteAsync(sql);
            return tableName;
        }

        class Foo : IEquatable<Foo>
        {
            [Key]
            public int Id { get; set; }
            public string Name { get; set; }

            public bool Equals(Foo other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Id == other.Id && Name == other.Name;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((Foo) obj);
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(Id, Name);
            }

            public static bool operator ==(Foo left, Foo right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(Foo left, Foo right)
            {
                return !Equals(left, right);
            }
        }
    }
}