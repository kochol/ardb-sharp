using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace ArdbSharp
{
    public class FireAndForget
    {
        public Connection MainConnection;

        private async ValueTask ExecuteAsync(string databaseName, string cmd, object arg)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.ExecuteAsync(cmd, arg);
        }

        public void Execute(string databaseName, string cmd, object arg)
        {
            Task.Run(() => ExecuteAsync(databaseName, cmd, arg));
        }

        private async ValueTask HashDeleteAsync(string databaseName, object key, object field)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.HashDeleteAsync(key, field);
        }

        public void HashDelete(string databaseName, object key, object field)
        {
            Task.Run(() => HashDeleteAsync(databaseName, key, field));
        }

        private async ValueTask HashIncrementAsync(string databaseName, object key, object field, long value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.HashIncrementAsync(key, field, value);
        }

        public void HashIncrement(string databaseName, object key, object field, long value)
        {
            Task.Run(() => HashIncrementAsync(databaseName, key, field, value));
        }
       
        private async ValueTask HashSetAsync(string databaseName, object key, params object[] fieldsAndValues)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.HashSetAsync(key, fieldsAndValues);
        }

        public void HashSet(string databaseName, object key, params object[] fieldsAndValues)
        {
            Task.Run(() => HashSetAsync(databaseName, key, fieldsAndValues));
        }

        private async ValueTask KeyDeleteAsync(string databaseName, object key)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.KeyDeleteAsync(key);
        }

        public void KeyDelete(string databaseName, object key)
        {
            Task.Run(() => KeyDeleteAsync(databaseName, key));
        }

        private async ValueTask ListLeftPushAsync(string databaseName, object key, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.ListLeftPushAsync(key, value);
        }

        public void ListLeftPush(string databaseName, object key, object value)
        {
            Task.Run(() => ListLeftPushAsync(databaseName, key, value));
        }

        private async ValueTask ListRightPushAsync(string databaseName, object key, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.ListRightPushAsync(key, value);
        }

        public void ListRightPush(string databaseName, object key, object value)
        {
            Task.Run(() => ListRightPushAsync(databaseName, key, value));
        }

        private async ValueTask ListRemoveAsync(string databaseName, object key, long count, object element)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.ListRemoveAsync(key, count, element);
        }

        public void ListRemove(string databaseName, object key, long count, object element)
        {
            Task.Run(() => ListRemoveAsync(databaseName, key, count, element));
        }

        private async ValueTask ListTrimAsync(string databaseName, object key, long start, long stop)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.ListTrimAsync(key, start, stop);
        }

        public void ListTrim(string databaseName, object key, long start, long stop)
        {
            Task.Run(() => ListTrimAsync(databaseName, key, start, stop));
        }

        private async ValueTask SortedSetAddAsync(string databaseName, object key, object member, double score)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.SortedSetAddAsync(key, member, score);
        }

        public void SortedSetAdd(string databaseName, object key, object member, double score)
        {
            Task.Run(() => SortedSetAddAsync(databaseName, key, member, score));
        }

        private async ValueTask StringAppendAsync(string databaseName, object key, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.StringAppendAsync(key, value);
        }

        public void StringAppend(string databaseName, object key, object value)
        {
            Task.Run(() => StringAppendAsync(databaseName, key, value));
        }

        private async ValueTask StringSetAsync(string databaseName, object key, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.StringSetAsync(key, value);
        }

        public void StringSet(string databaseName, object key, object value)
        {
            Task.Run(() => StringSetAsync(databaseName, key, value));
        }

        private async ValueTask StringSetAsync(string databaseName, object key, object value, int ExpireSeconds)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.StringSetAsync(key, value, ExpireSeconds);
        }

        public void StringSet(string databaseName, object key, object value, int ExpireSeconds)
        {
            Task.Run(() => StringSetAsync(databaseName, key, value, ExpireSeconds));
        }

    }
}