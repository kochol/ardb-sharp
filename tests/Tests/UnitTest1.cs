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
    }
}