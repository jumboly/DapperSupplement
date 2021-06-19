using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace DapperSupplement.Test.Tests
{
    public class EnumerableExtensionsTest
    {
        [Fact]
        public void TakeByEmpty()
        {
            var actual = Array.Empty<int>().TakeBy(10);
            Assert.Empty(actual);
        }

        [Fact]
        public void TakeByOne()
        {
            var actual = new[] {1}.TakeBy(1);
            var expected = new[] {new[] {1}};
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TakeByJust()
        {
            var actual = new[] {1, 2, 3, 4}.TakeBy(2);
            var expected = new[] {new[] {1, 2}, new[] {3, 4}};
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public void TakeByTest()
        {
            var actual = new[] {1, 2, 3, 4, 5}.TakeBy(2);
            var expected = new[] {new[] {1, 2}, new[] {3, 4}, new[] {5}};
            Assert.Equal(expected, actual);
        }
    }
}