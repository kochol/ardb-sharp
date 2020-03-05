using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace ArdbSharp
{
    public static class FireAndForget
    {
        public static Connection MainConnection;

        private static async Task HashDeleteAsync(string databaseName, object key, params object[] fields)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.HashDeleteAsync(key, fields);
        }

        public static void HashDelete(string databaseName, object key, params object[] fields)
        {
            Task.Run(() => HashDeleteAsync(databaseName, key, fields));
        }

        private static async Task HashIncrementAsync(string databaseName, object key, object field, long value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.HashIncrementAsync(key, field, value);
        }

        public static void HashIncrement(string databaseName, object key, object field, long value)
        {
            Task.Run(() => HashIncrementAsync(databaseName, key, field, value));
        }

        private static async Task HashSetAsync(string databaseName, object key, object field, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.HashSetAsync(key, field, value);
        }

        public static void HashSet(string databaseName, object key, object field, object value)
        {
            Task.Run(() => HashSetAsync(databaseName, key, field, value));
        }

        private static async Task KeyDeleteAsync(string databaseName, object key)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.KeyDeleteAsync(key);
        }

        public static void KeyDelete(string databaseName, object key)
        {
            Task.Run(() => KeyDeleteAsync(databaseName, key));
        }

        private static async Task ListRightPushAsync(string databaseName, object key, params object[] elements)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.ListRightPushAsync(key, elements);
        }

        public static void ListRightPush(string databaseName, object key, params object[] elements)
        {
            Task.Run(() => ListRightPushAsync(databaseName, key, elements));
        }

        private static async Task SortedSetAddAsync(string databaseName, object key, object member, double score)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.SortedSetAddAsync(key, member, score);
        }

        public static void SortedSetAdd(string databaseName, object key, object member, double score)
        {
            Task.Run(() => SortedSetAddAsync(databaseName, key, member, score));
        }

        private static async Task StringAppendAsync(string databaseName, object key, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.StringAppendAsync(key, value);
        }

        public static void StringAppend(string databaseName, object key, object value)
        {
            Task.Run(() => StringAppendAsync(databaseName, key, value));
        }

        private static async Task StringSetAsync(string databaseName, object key, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.StringSetAsync(key, value);
        }

        public static void StringSet(string databaseName, object key, object value)
        {
            Task.Run(() => StringSetAsync(databaseName, key, value));
        }

    }
}