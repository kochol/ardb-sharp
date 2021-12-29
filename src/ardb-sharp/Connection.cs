using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Respite;

namespace ArdbSharp
{
    public class Connection: IDisposable
    {
        private readonly ConnectionConfig _config;

        private readonly ConcurrentStack<Database> _databases = new ConcurrentStack<Database>();
        private SemaphoreSlim semaphoreSlim;

        private long _count;

        public long Count => Interlocked.Read(ref _count);

        public Connection(ConnectionConfig config)
        {
            _config = config;
            semaphoreSlim = new SemaphoreSlim(config.MaxConnections, config.MaxConnections);
        }

        public void Dispose()
        {
            while (_databases.Count > 0)
            {            
                Database db;
                if (_databases.TryPop(out db))
                {
                    db.Dispose();
                }
            }
            
            semaphoreSlim.Dispose();
        }

        private static void AddDatabaseToStack(Database db, Connection connection)
        {
            try
            {
                connection._databases.Push(db);
                connection.semaphoreSlim.Release();
            }
            catch
            {

            }
        }

        private static readonly Action<Database, object?> s_ReturnToDatabasePool =
            (_, connection) => AddDatabaseToStack(_, (Connection) connection);


        public async ValueTask<Lifetime<Database>> GetDatabaseAsync(string databaseName)
        {
            try
            {
                await semaphoreSlim.WaitAsync();

                Database db_;
                if (_databases.TryPop(out db_))
                {
                    await db_.Select(databaseName);
                    return new Lifetime<Database>(db_, s_ReturnToDatabasePool, this);
                }

                Interlocked.Increment(ref _count);
                var db = await Database.ConnectAsync(_config.EndPoint, databaseName);
                return new Lifetime<Database>(db, s_ReturnToDatabasePool, this);
            }
            catch (Exception e)
            {
                semaphoreSlim.Release();
                if (e is EndOfStreamException)
                    return await GetDatabaseAsync(databaseName);
                else
                    throw e;
            }
            finally
            {
            }
        }

    }
}