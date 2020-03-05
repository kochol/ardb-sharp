using System;
using System.Collections.Generic;
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

        private readonly Dictionary<string, Stack<Database>> _databases = new Dictionary<string, Stack<Database>>();

        public Connection(ConnectionConfig config)
        {
            _config = config;
        }

        private static SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);

        private static void AddDatabaseToStack(Database db, Connection connection)
        {
            connection._databases[db.DatabaseName].Push(db);
        }

        private static readonly Action<Database, object?> s_ReturnToDatabasePool =
            (_, connection) => AddDatabaseToStack(_, (Connection) connection);


        public async ValueTask<Lifetime<Database>> GetDatabaseAsync(string databaseName)
        {
            await _semaphoreSlim.WaitAsync();

            try
            {
                if (_databases.ContainsKey(databaseName))
                {
                    if (_databases[databaseName].Count > 0)
                    {
                        return new Lifetime<Database>(_databases[databaseName].Pop(), s_ReturnToDatabasePool, this);
                    }
                }
                else
                {
                    _databases[databaseName] = new Stack<Database>();
                }

                var db = await Database.ConnectAsync(_config.EndPoint, databaseName);
                return new Lifetime<Database>(db, s_ReturnToDatabasePool, this);
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }
    }
}