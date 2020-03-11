using System.Net;

namespace ArdbSharp
{
    public class ConnectionConfig
    {
        public EndPoint EndPoint;
        public int MaxConnections = 5000;
    }
}