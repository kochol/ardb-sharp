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

        public string DatabaseName { get; private set; }

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

        public async ValueTask<object> HashDeleteAsync(object key, object field)
        {
            return await _connection.CallAsync("HDEL", key, field);
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
            return await _connection.CallAsync("HGET", key, field);
        }

        public async ValueTask<object> HashIncrementAsync(object key, object field, long value)
        {
            return await _connection.CallAsync("HINCRBY", key, field, value);
        }

        public async ValueTask<object> HashSetAsync(object key, params object[] fieldsAndValues)
        {
            var o = new object[fieldsAndValues.Length + 1];
            o[0] = key;
            for (int i = 0; i < fieldsAndValues.Length; i++)
                o[i + 1] = fieldsAndValues[i];
            return await _connection.CallAsync("HSET", o);
        }

        public async ValueTask<HashScan> HashScanAsync(object key, object pattern = null, int pageSize = 10, long cursor = 0L)
        {
            return new HashScan(await hashScanAsync(key, pattern, pageSize, cursor));
        }

        private async ValueTask<object> hashScanAsync(object key, object pattern = null, int pageSize = 10, long cursor = 0L)
        {
            if (pattern == null)
                return await _connection.CallAsync("HSCAN", key, cursor, "COUNT", pageSize);
            return await _connection.CallAsync("HSCAN", key, cursor, "MATCH", pattern, "COUNT", pageSize);
        }

        public async ValueTask<object> KeyDeleteAsync(object key)
        {
            return await _connection.CallAsync("DEL", key);
        }

        public async ValueTask<object> ListLeftPopAsync(object key)
        {
            return await _connection.CallAsync("LPOP", key);
        }

        public async ValueTask<object> ListLenghtAsync(object key)
        {
            return await _connection.CallAsync("LLEN", key);
        }

        public async ValueTask<object[]> ListRangeAsync(object key, long start, long end)
        {
            return (object[])(await _connection.CallAsync("LRANGE", key, start, end));
        }

        public async ValueTask<object> ListLeftPushAsync(object key, object value)
        {
            return await _connection.CallAsync("LPUSH", key, value);
        }

        public async ValueTask<object> ListRightPushAsync(object key, object value)
        {
            return await _connection.CallAsync("RPUSH", key, value);
        }

        public async ValueTask<object> ListRemoveAsync(object key, long count, object element)
        {
            return await _connection.CallAsync("LREM", key, count, element);
        }

        public async ValueTask<object> ListTrimAsync(object key, long start, long stop)
        {
            return await _connection.CallAsync("LTRIM", key, start, stop);
        }

        public async ValueTask<object> Select(string dbName)
        {
            if (dbName == DatabaseName)
                return "OK";
            DatabaseName = dbName;
            return await _connection.CallAsync("SELECT", dbName);
        }

        public async ValueTask<object> SortedSetAddAsync(object key, object member, double score)
        {
            return await _connection.CallAsync("ZADD", key, score, member);
        }

        public async ValueTask<object> SortedSetScanAsync(object key, object cursur)
        {
            return await _connection.CallAsync("ZSCAN", key, cursur);
        }

        public async ValueTask<object> SortedSetRangeAsync(object key, object start, object end)
        {
            return await _connection.CallAsync("ZRANGE", key, start, end);
        }

        public async ValueTask<object> StringAppendAsync(object key, object value)
        {
            return await _connection.CallAsync("APPEND", key, value);
        }

        public static async ValueTask<object> StringGetAsync(Connection connection, string databaseName, object key)
        {
            using var db = await connection.GetDatabaseAsync(databaseName);
            return await db.Value.StringGetAsync(key);
        }

        public async ValueTask<object> StringGetAsync(object key)
        {
            return await _connection.CallAsync("GET", key);
        }

        public async ValueTask<long> StringIncr(object key, long by)
        {
            return (long)await _connection.CallAsync("INCRBY", key, by);
        }

        public async ValueTask<object> StringSetAsync(object key, object value)
        {
            return await _connection.CallAsync("SET", key, value);
        }

        public async ValueTask<object> StringSetAsync(object key, object value, int ExpireSeconds)
        {
            return await _connection.CallAsync("SET", key, value, ExpireSeconds);
        }

        public async ValueTask<object> ExecuteAsync(string cmd, object arg)
        {
            return await _connection.CallAsync(cmd, arg);
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

            if (byteArray is long)
                return (long)byteArray;

            return long.Parse(System.Text.Encoding.UTF8.GetString((byte[])byteArray));
        }
    }
}