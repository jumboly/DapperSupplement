using System.ComponentModel.DataAnnotations;
using Xunit;

namespace DapperSupplement.Test.Tests
{
    public class SqlBuilderTest
    {
        [Fact]
        public void CreateInsertTest()
        {
            var actual = SqlBuilder.CreateInsert(typeof(Foo), "foo");
            var expected = "insert into foo (Id,Name) values (@Id,@Name)";
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void CreateUpdateTest()
        {
            var actual = SqlBuilder.CreateUpdate(typeof(Foo), "foo");
            var expected = "update foo set Name=@Name where Id=@Id";
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void CreateUpdateTest2()
        {
            // 複合キーのテスト
            var actual = SqlBuilder.CreateUpdate(typeof(Foo2), "foo");
            var expected = "update foo set Name=@Name where Id1=@Id1 and Id2=@Id2";
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void CreateDeleteTest()
        {
            var actual = SqlBuilder.CreateDelete(typeof(Foo), "foo");
            var expected = "delete from foo where Id=@Id";
            Assert.Equal(expected, actual);            
        }

        [Fact]
        public void CreateDeleteTest2()
        {
            var actual = SqlBuilder.CreateDelete(typeof(Foo2), "foo");
            var expected = "delete from foo where Id1=@Id1 and Id2=@Id2";
            Assert.Equal(expected, actual);            
        }

        class Foo
        {
            [Key]
            public int Id { get; set; }
            public string Name { get; set; }
        }

        class Foo2
        {
            [Key]
            public int Id1 { get; set; }
            [Key]
            public int Id2 { get; set; }
            public string Name { get; set; }
        }
    }
}