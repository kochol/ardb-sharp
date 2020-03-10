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
    public class Connection
    {
        private readonly ConnectionConfig _config;

        private readonly ConcurrentDictionary<string, ConcurrentStack<Database>> _databases = new ConcurrentDictionary<string, ConcurrentStack<Database>>(16, 5);

        public Connection(ConnectionConfig config)
        {
            _config = config;
        }

        private static void AddDatabaseToStack(Database db, Connection connection)
        {
            if (connection._databases[db.DatabaseName].Count < 20)
                connection._databases[db.DatabaseName].Push(db);
            else
                db.Dispose();
        }

        private static readonly Action<Database, object?> s_ReturnToDatabasePool =
            (_, connection) => AddDatabaseToStack(_, (Connection) connection);


        public async ValueTask<Lifetime<Database>> GetDatabaseAsync(string databaseName)
        {
            try
            {
                if (_databases.ContainsKey(databaseName))
                {
                    Database db_;
                    if (_databases[databaseName].TryPop(out db_))
                    {

                        return new Lifetime<Database>(db_, s_ReturnToDatabasePool, this);
                    }
                }
                else
                {
                    _databases[databaseName] = new ConcurrentStack<Database>();
                }

                var db = await Database.ConnectAsync(_config.EndPoint, databaseName);
                return new Lifetime<Database>(db, s_ReturnToDatabasePool, this);
            }
            catch (Exception e)
            {
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