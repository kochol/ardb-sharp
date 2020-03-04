using System.Collections.Generic;
using System.Threading.Tasks;

namespace ArdbSharp
{
    public class Connection
    {
        private readonly ConnectionConfig _config;

        private readonly Dictionary<string, Database> _databases = new Dictionary<string, Database>();

        public Connection(ConnectionConfig config)
        {
            _config = config;
        }

        private readonly object _addDbLock = new object();

        public Database GetDatabase(string databaseName)
        {
            if (_databases.ContainsKey(databaseName))
                return _databases[databaseName];

            lock (_addDbLock)
            {
                var db = Database.ConnectAsync(_config.EndPoint, databaseName).Result;
                _databases[databaseName] = db;
                return db;
            }
        }
    }
}