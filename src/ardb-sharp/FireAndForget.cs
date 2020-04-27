using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace ArdbSharp
{
    public class FireAndForget
    {
        public Connection MainConnection;

        private async Task ExecuteAsync(string databaseName, string cmd, object arg)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.ExecuteAsync(cmd, arg);
        }

        public void Execute(string databaseName, string cmd, object arg)
        {
            Task.Run(() => ExecuteAsync(databaseName, cmd, arg));
        }

        private async Task HashDeleteAsync(string databaseName, object key, object field)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.HashDeleteAsync(key, field);
        }

        public void HashDelete(string databaseName, object key, object field)
        {
            Task.Run(() => HashDeleteAsync(databaseName, key, field));
        }

        private async Task HashIncrementAsync(string databaseName, object key, object field, long value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.HashIncrementAsync(key, field, value);
        }

        public void HashIncrement(string databaseName, object key, object field, long value)
        {
            Task.Run(() => HashIncrementAsync(databaseName, key, field, value));
        }

        private async Task HashSetAsync(string databaseName, object key, object field, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.HashSetAsync(key, field, value);
        }

        public void HashSet(string databaseName, object key, object field, object value)
        {
            Task.Run(() => HashSetAsync(databaseName, key, field, value));
        }

        private async Task KeyDeleteAsync(string databaseName, object key)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.KeyDeleteAsync(key);
        }

        public void KeyDelete(string databaseName, object key)
        {
            Task.Run(() => KeyDeleteAsync(databaseName, key));
        }

        private async Task ListRightPushAsync(string databaseName, object key, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.ListRightPushAsync(key, value);
        }

        public void ListRightPush(string databaseName, object key, object value)
        {
            Task.Run(() => ListRightPushAsync(databaseName, key, value));
        }

        private async Task SortedSetAddAsync(string databaseName, object key, object member, double score)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.SortedSetAddAsync(key, member, score);
        }

        public void SortedSetAdd(string databaseName, object key, object member, double score)
        {
            Task.Run(() => SortedSetAddAsync(databaseName, key, member, score));
        }

        private async Task StringAppendAsync(string databaseName, object key, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.StringAppendAsync(key, value);
        }

        public void StringAppend(string databaseName, object key, object value)
        {
            Task.Run(() => StringAppendAsync(databaseName, key, value));
        }

        private async Task StringSetAsync(string databaseName, object key, object value)
        {
            using var db = await MainConnection.GetDatabaseAsync(databaseName);
            await db.Value.StringSetAsync(key, value);
        }

        public void StringSet(string databaseName, object key, object value)
        {
            Task.Run(() => StringSetAsync(databaseName, key, value));
        }

    }
}