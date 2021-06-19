using System;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DapperSupplement.Test.Tests
{
    public class DbConnectionExtensionsTest
    {
        private readonly SqliteConnection _connection ;
        public DbConnectionExtensionsTest()
        {
            var connectionString = new SqliteConnectionStringBuilder()
            {
                DataSource = Guid.NewGuid().ToString(),
                Mode = SqliteOpenMode.Memory,
                Cache = SqliteCacheMode.Shared
            }.ConnectionString;
            _connection = new SqliteConnection(connectionString);
            _connection.Open();
        }

        [Fact]
        public async Task Insert()
        {
            var table = await Foo.CreateTableAsync(_connection);

            var foo = new Foo {Id = 1, Name = "abc"};
            var count = await _connection.InsertAsync<Foo>(table, foo);
            Assert.Equal(1, count);

            var foo2 = _connection.QueryFirst<Foo>($"select * from {table} where Id = 1");
            Assert.NotNull(foo2);
            Assert.Equal(foo, foo2);
        }

        [Fact]
        public async Task BatchInsert()
        {
            var table = await Foo.CreateTableAsync(_connection);

            var foos = new[]
            {
                new Foo() {Id = 1, Name = "a"},
                new Foo() {Id = 2, Name = "b"},
                new Foo() {Id = 3, Name = "c"},
            };
            var count = await _connection.BatchInsertAsync(table, foos, 2);
            Assert.Equal(3, count);

            var foos2 = (await _connection.QueryAsync<Foo>($"select * from {table} order by Id")).ToArray();
            Assert.Equal(3, foos2.Length);
            Assert.Equal(foos, foos2);
        }

        [Fact]
        public async Task Update()
        {
            var table = await Foo.CreateTableAsync(_connection);

            var foo = new Foo {Id = 1, Name = "abc"};
            await _connection.InsertAsync(table, foo);

            foo.Name = "def";
            var count = await _connection.UpdateAsync(table, foo);
            Assert.Equal(1, count);

            var foo2 = await _connection.QueryFirstAsync<Foo>($"select * from {table} where id = 1");
            Assert.NotNull(foo2);
            Assert.Equal(1, foo2.Id);
            Assert.Equal("def", foo2.Name);
        }

        [Fact]
        public async Task BatchUpdate()
        {
            var table = await Foo.CreateTableAsync(_connection);

            var foos = new[]
            {
                new Foo() {Id = 1, Name = "a"},
                new Foo() {Id = 2, Name = "b"},
                new Foo() {Id = 3, Name = "c"},
            };
            await _connection.BatchInsertAsync(table, foos);

            foos[0].Name = "x";
            foos[1].Id = 999; foos[1].Name = "y"; // 存在しないので Update で空振りする
            foos[2].Name = "z";
            var count = await _connection.BatchUpdateAsync(table, foos, 2);
            Assert.Equal(2, count);
            
            var foos2 = (await _connection.QueryAsync<Foo>($"select * from {table} order by id")).ToArray();
            var expected = new[]
            {
                new Foo() {Id = 1, Name = "x"},
                new Foo() {Id = 2, Name = "b"}, // 空振りしているので更新されない
                new Foo() {Id = 3, Name = "z"},
            };
            Assert.Equal(expected, foos2);
        }

        [Fact]
        public async Task Delete()
        {
            var table = await Foo.CreateTableAsync(_connection);

            var foos = new[]{
                new Foo {Id = 1, Name = "a"},
                new Foo {Id = 2, Name = "b"},
                new Foo {Id = 3, Name = "c"}
            };
            await _connection.BatchInsertAsync(table, foos);

            var count = await _connection.DeleteAsync(table, foos[1]); // Id=2を消す
            Assert.Equal(1, count);

            var foos2 = (await _connection.QueryAsync<Foo>($"select * from {table}")).ToArray();
            var expected = new[]
            {
                new Foo {Id = 1, Name = "a"},
                new Foo {Id = 3, Name = "c"},
            };
            Assert.Equal(expected, foos2);
        }

        [Fact]
        public async Task BatchDelete()
        {
            var table = await Foo.CreateTableAsync(_connection);

            var foos = new[]{
                new Foo {Id = 1, Name = "a"},
                new Foo {Id = 2, Name = "b"},
                new Foo {Id = 3, Name = "c"},
                new Foo {Id = 4, Name = "d"},
                new Foo {Id = 5, Name = "e"},
            };
            await _connection.BatchInsertAsync(table, foos);

            var count = await _connection.BatchDeleteAsync(table, new []{foos[0], foos[1], foos[3]}, 2);
            Assert.Equal(3, count);

            var foos2 = (await _connection.QueryAsync<Foo>($"select * from {table}")).ToArray();
            var expected = new[]
            {
                new Foo {Id = 3, Name = "c"},
                new Foo {Id = 5, Name = "e"},
            };
            Assert.Equal(expected, foos2);
        }
        
        class Foo : IEquatable<Foo>
        {
            public static async Task<string> CreateTableAsync(IDbConnection connection)
            {
                var tableName = $"foo_{Guid.NewGuid():N}";
                await connection.ExecuteAsync($"create table {tableName} (id primary key, name)");
                return tableName;
            }
            
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