using System.Net;

namespace ArdbSharp
{
    public class ConnectionConfig
    {
        public EndPoint EndPoint;
        public int MaxConnections = 1000;
        public int ConnectionLimitTimeOut = 15000; // 15 seconds
    }
}