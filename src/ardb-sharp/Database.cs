﻿using System;
using System.Net;
using System.Threading.Tasks;
using Respite.Redis;

namespace ArdbSharp
{
    public class Database : IDisposable
    {
        private readonly RedisConnection _connection;

        public string DatabaseName { get; }

        private Database(RedisConnection connection, string databaseName)
        {
            _connection = connection;
            DatabaseName = databaseName;
        }

        public static async ValueTask<Database> ConnectAsync(EndPoint endpoint, string databaseName)
        {
            var connection = await RedisConnection.ConnectAsync(endpoint);
            // set the connection database.
            await connection.CallAsync("SELECT", databaseName);
            return new Database(connection, databaseName);
        }

        public void Dispose() => _connection.Dispose();

        public async ValueTask<object> StringGetAsync(object key)
        {
            return await _connection.CallAsync("GET", key);
        }

        public async ValueTask<object> StringSetAsync(object key, object value)
        {
            return await _connection.CallAsync("SET", key, value);
        }
    }
}