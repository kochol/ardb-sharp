using System;
using System.Net;
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
        public void TestStringGetSet()
        {
            var db = _connection.GetDatabase("1");
            db.StringSetAsync("foo", "bar").GetAwaiter();
            var bar = db.StringGetAsync("foo").Result;
            Assert.True(bar.ToString() == "bar");
        }
    }
}