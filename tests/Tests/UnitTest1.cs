using System;
using System.Net;
using System.Text;
using System.Threading;
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
            using var db = await _connection.GetDatabaseAsync("0");
            await db.Value.StringSetAsync("foo", "bar");
            var bar = await db.Value.StringGetAsync("foo");
            Assert.True(Database.ToString(bar) == "bar");

            var notFound = await db.Value.StringGetAsync("foo2");
            Assert.True(notFound == null);
        }

        [Test]
        public void FireAndForgetTest()
        {
            FireAndForget.MainConnection = _connection;
            for (int i = 0; i < 20; i++)
            {
                FireAndForget.StringAppend("0", "str", i);
                FireAndForget.ListRightPush("0", "temp", i);
            }
            Thread.Sleep(1000);
        }

        [Test]
        public async Task ListTest()
        {
            using var db = await _connection.GetDatabaseAsync("0");
            var r = await db.Value.ListRightPushAsync("temp", 1);
            var r2 = await db.Value.ListLeftPopAsync("temp");
            if (r2 == null)
                Assert.Fail();
            Assert.Pass();
        }

        [Test]
        public async Task ByteTest()
        {
            using var db = await _connection.GetDatabaseAsync("0");
            int byteCount = 100;
            byte[] bts = new byte[byteCount];
            for (int i = 0; i < byteCount; i++)
                bts[i] = (byte)(i % 128);
            await db.Value.StringSetAsync("bts", bts);
            byte[] r = (byte[])await db.Value.StringGetAsync("bts");
            for (int i = 0; i < byteCount; i++)
                Assert.True(bts[i] == r[i]);
        }

        [Test]
        public async Task HashTest()
        {
            using var db = await _connection.GetDatabaseAsync("0");
            int c = 5;
            for (int i = 0; i < c; i++)
                await db.Value.HashSetAsync("h1", "f" + i, i);
            var r = await db.Value.HashGetAllAsync("h1");
            for (int i = 0; i < c; i++)
            {
                Assert.True(r[i].Name == "f" + i);
                Assert.True(Database.ToInt(r[i].Value) == i);
            }
        }

        [Test]
        public async Task SortedSetTest()
        {
            using var db = await _connection.GetDatabaseAsync("1");
            await db.Value.SortedSetAddAsync("zset", "two", 2);
            await db.Value.SortedSetAddAsync("zset", "four", 4);
            await db.Value.SortedSetAddAsync("zset", "three", 3);
            await db.Value.SortedSetAddAsync("zset", "one", 1);
            await db.Value.SortedSetAddAsync("zset", "five", 5);
            await db.Value.SortedSetAddAsync("zset", "zero", 0);
            await db.Value.SortedSetAddAsync("zset", "une", 1);
            await db.Value.SortedSetAddAsync("zset", "tone", 4);
            await db.Value.SortedSetAddAsync("zset", "waone", 6);
            await db.Value.SortedSetAddAsync("zset", "ttone", 8);
            await db.Value.SortedSetAddAsync("zset", "3tone", 9);
            await db.Value.SortedSetAddAsync("zset", "534one", 7);
            await db.Value.SortedSetAddAsync("zset", "twdo", 2);
            await db.Value.SortedSetAddAsync("zset", "foudr", 4);
            await db.Value.SortedSetAddAsync("zset", "thrdee", 3);
            await db.Value.SortedSetAddAsync("zset", "oned", 1);
            await db.Value.SortedSetAddAsync("zset", "fivde", 5);
            await db.Value.SortedSetAddAsync("zset", "zerdo", 0);
            await db.Value.SortedSetAddAsync("zset", "uned", 1);
            await db.Value.SortedSetAddAsync("zset", "tonde", 4);
            await db.Value.SortedSetAddAsync("zset", "waodne", 6);
            await db.Value.SortedSetAddAsync("zset", "ttodne", 8);
            await db.Value.SortedSetAddAsync("zset", "3todne", 9);
            await db.Value.SortedSetAddAsync("zset", "534done", 7);
            await db.Value.SortedSetAddAsync("zset", "twro", 2);
            await db.Value.SortedSetAddAsync("zset", "forur", 4);
            await db.Value.SortedSetAddAsync("zset", "thrree", 3);
            await db.Value.SortedSetAddAsync("zset", "onre", 1);
            await db.Value.SortedSetAddAsync("zset", "firve", 5);
            await db.Value.SortedSetAddAsync("zset", "zerro", 0);
            await db.Value.SortedSetAddAsync("zset", "unre", 1);
            await db.Value.SortedSetAddAsync("zset", "torne", 4);
            await db.Value.SortedSetAddAsync("zset", "warone", 6);
            await db.Value.SortedSetAddAsync("zset", "ttrone", 8);
            await db.Value.SortedSetAddAsync("zset", "3trone", 9);
            await db.Value.SortedSetAddAsync("zset", "53r4one", 7);
            await db.Value.SortedSetAddAsync("zset", "twto", 2);
            await db.Value.SortedSetAddAsync("zset", "fotur", 4);
            await db.Value.SortedSetAddAsync("zset", "thtree", 3);
            await db.Value.SortedSetAddAsync("zset", "onte", 1);
            await db.Value.SortedSetAddAsync("zset", "fitve", 5);
            await db.Value.SortedSetAddAsync("zset", "zetro", 0);
            await db.Value.SortedSetAddAsync("zset", "unte", 1);
            await db.Value.SortedSetAddAsync("zset", "totne", 4);
            await db.Value.SortedSetAddAsync("zset", "watone", 6);
            await db.Value.SortedSetAddAsync("zset", "tttone", 8);
            await db.Value.SortedSetAddAsync("zset", "3ttone", 9);
            await db.Value.SortedSetAddAsync("zset", "53t4one", 7);
            var r = await db.Value.SortedSetScanAsync("zset", 0);

            Assert.Pass(r.ToString());
        }
    }
}