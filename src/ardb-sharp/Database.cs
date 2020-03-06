using System;
using System.Collections.Generic;
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

        public async ValueTask<object> HashDeleteAsync(object key, params object[] fields)
        {
            return await _connection.CallAsync("HDEL", key, fields);
        }

        public async ValueTask<List<(string Name, object Value)>> HashGetAllAsync(object key)
        {
            var r = await _connection.CallAsync("HGETALL", key);
            if (r == null)
                return null;

            var a = (object[])r;
            var l = new List<(string Name, object Value)>(a.Length / 2);
            for (int i = 0; i < a.Length; i += 2)
            {
                l.Add((Name: Database.ToString(a[i]), Value: a[i + 1]));
            }
            return l;
        }

        public async ValueTask<object> HashGetAsync(object key, object field)
        {
            return await _connection.CallAsync("HGETALL", key, field);
        }

        public async ValueTask<object> HashIncrementAsync(object key, object field, long value)
        {
            return await _connection.CallAsync("HINCRBY", key, field, value);
        }

        public async ValueTask<object> HashSetAsync(object key, object field, object value)
        {
            return await _connection.CallAsync("HSET", key, field, value);
        }

        public async ValueTask<object> KeyDeleteAsync(object key)
        {
            return await _connection.CallAsync("DEL", key);
        }

        public async ValueTask<object> ListLeftPopAsync(object key)
        {
            return await _connection.CallAsync("LPOP", key);
        }

        public async ValueTask<object> ListRightPushAsync(object key, params object[] elements)
        {
            return await _connection.CallAsync("RPUSH", key, elements);
        }

        public async ValueTask<object> SortedSetAddAsync(object key, object member, double score)
        {
            return await _connection.CallAsync("ZADD", key, score, member);
        }

        public async ValueTask<object> SortedSetScanAsync(object key, object cursur)
        {
            return await _connection.CallAsync("ZSCAN", key, cursur);
        }

        public async ValueTask<object> StringAppendAsync(object key, object value)
        {
            return await _connection.CallAsync("APPEND", key, value);
        }

        public async ValueTask<object> StringGetAsync(object key)
        {
            return await _connection.CallAsync("GET", key);
        }

        public async ValueTask<object> StringSetAsync(object key, object value)
        {
            return await _connection.CallAsync("SET", key, value);
        }

        public async ValueTask<object> ExecuteAsync(string cmd, params object[] args)
        {
            return await _connection.CallAsync(cmd, args);
        }

        public static string ToString(object byteArray)
        {
            if (byteArray == null)
                return null;

            if (byteArray is string)
                return (string)byteArray;

            return System.Text.Encoding.UTF8.GetString((byte[])byteArray);
        }

        public static int ToInt(object byteArray)
        {
            if (byteArray == null)
                return 0;

            return int.Parse(System.Text.Encoding.UTF8.GetString((byte[])byteArray));
        }

        public static long ToLong(object byteArray)
        {
            if (byteArray == null)
                return 0;

            return long.Parse(System.Text.Encoding.UTF8.GetString((byte[])byteArray));
        }
    }
}