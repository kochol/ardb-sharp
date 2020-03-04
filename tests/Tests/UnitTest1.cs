using System;
using System.Net;
using System.Threading.Tasks;
using ArdbSharp;
using NUnit.Framework;

namespace Tests
{
    public class Tests
    {
        private Connection _connection;

        [SetUp]
        public void Setup()
        {
            var config = new ConnectionConfig();
            config.EndPoint = new IPEndPoint(IPAddress.Loopback, 16379);

            _connection = new Connection(config);
        }

        [Test]
        public async Task TestStringGetSet()
        {
            var db = _connection.GetDatabase("0");
            await db.StringSetAsync("foo", "bar");
            var bar = await db.StringGetAsync("foo");
            Assert.True(bar.ToString() == "bar");

            var notFound = await db.StringGetAsync("foo2");
            Assert.True(string.IsNullOrEmpty(notFound.ToString()));
        }

        [Test]
        public async Task SortedSetTest()
        {
            var db = _connection.GetDatabase("1");
            await db.SortedSetAddAsync("zset", "two", 2);
            await db.SortedSetAddAsync("zset", "four", 4);
            await db.SortedSetAddAsync("zset", "three", 3);
            await db.SortedSetAddAsync("zset", "one", 1);
            await db.SortedSetAddAsync("zset", "five", 5);
            await db.SortedSetAddAsync("zset", "zero", 0);
            await db.SortedSetAddAsync("zset", "une", 1);
            await db.SortedSetAddAsync("zset", "tone", 4);
            await db.SortedSetAddAsync("zset", "waone", 6);
            await db.SortedSetAddAsync("zset", "ttone", 8);
            await db.SortedSetAddAsync("zset", "3tone", 9);
            await db.SortedSetAddAsync("zset", "534one", 7);
            await db.SortedSetAddAsync("zset", "twdo", 2);
            await db.SortedSetAddAsync("zset", "foudr", 4);
            await db.SortedSetAddAsync("zset", "thrdee", 3);
            await db.SortedSetAddAsync("zset", "oned", 1);
            await db.SortedSetAddAsync("zset", "fivde", 5);
            await db.SortedSetAddAsync("zset", "zerdo", 0);
            await db.SortedSetAddAsync("zset", "uned", 1);
            await db.SortedSetAddAsync("zset", "tonde", 4);
            await db.SortedSetAddAsync("zset", "waodne", 6);
            await db.SortedSetAddAsync("zset", "ttodne", 8);
            await db.SortedSetAddAsync("zset", "3todne", 9);
            await db.SortedSetAddAsync("zset", "534done", 7);
            await db.SortedSetAddAsync("zset", "twro", 2);
            await db.SortedSetAddAsync("zset", "forur", 4);
            await db.SortedSetAddAsync("zset", "thrree", 3);
            await db.SortedSetAddAsync("zset", "onre", 1);
            await db.SortedSetAddAsync("zset", "firve", 5);
            await db.SortedSetAddAsync("zset", "zerro", 0);
            await db.SortedSetAddAsync("zset", "unre", 1);
            await db.SortedSetAddAsync("zset", "torne", 4);
            await db.SortedSetAddAsync("zset", "warone", 6);
            await db.SortedSetAddAsync("zset", "ttrone", 8);
            await db.SortedSetAddAsync("zset", "3trone", 9);
            await db.SortedSetAddAsync("zset", "53r4one", 7);
            await db.SortedSetAddAsync("zset", "twto", 2);
            await db.SortedSetAddAsync("zset", "fotur", 4);
            await db.SortedSetAddAsync("zset", "thtree", 3);
            await db.SortedSetAddAsync("zset", "onte", 1);
            await db.SortedSetAddAsync("zset", "fitve", 5);
            await db.SortedSetAddAsync("zset", "zetro", 0);
            await db.SortedSetAddAsync("zset", "unte", 1);
            await db.SortedSetAddAsync("zset", "totne", 4);
            await db.SortedSetAddAsync("zset", "watone", 6);
            await db.SortedSetAddAsync("zset", "tttone", 8);
            await db.SortedSetAddAsync("zset", "3ttone", 9);
            await db.SortedSetAddAsync("zset", "53t4one", 7);
            var r = await db.SortedSetScanAsync("zset", 0);

            Assert.Pass(r.ToString());
        }
    }
}