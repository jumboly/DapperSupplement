using System.Linq;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DapperSupplement.SqlServer.Test.Tests
{
    public class SqlBulkCopyBuilderTest
    {
        [Fact]
        public void Create()
        {
            using var connection = new SqlConnection();

            using var actual = SqlBulkCopyBuilder.Create<Foo>(
                connection, "foo", SqlBulkCopyOptions.TableLock, null, 300, 1000);
            
            Assert.Equal("foo", actual.DestinationTableName);
            Assert.Equal(300, actual.BulkCopyTimeout);
            Assert.Equal(1000, actual.BatchSize);
            
            Assert.Collection(actual.ColumnMappings.Cast<SqlBulkCopyColumnMapping>(),
                it =>
                {
                    Assert.Equal("Id", it.SourceColumn);
                    Assert.Equal("Id", it.DestinationColumn);
                },
                it =>
                {
                    Assert.Equal("Name", it.SourceColumn);
                    Assert.Equal("Name", it.DestinationColumn);
                });
        }

        class Foo
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }
    }
}