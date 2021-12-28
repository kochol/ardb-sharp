using System;
using System.Collections.Generic;
using System.Text;

namespace ArdbSharp
{
    public class HashScan
    {
        private long _cursor;

        public long Cursor => _cursor;

        public HashEntry[] Entries;

        public HashScan(object res)
        {
            object[] ores = res as object[];
            _cursor = Database.ToLong(ores[0]);
            object[] h = ores[1] as object[];
            Entries = new HashEntry[h.Length / 2];
            for (int i = 0; i < h.Length; i+=2)
            {
                Entries[i / 2].SetHashEntry(Database.ToString(h[i]), h[i + 1]);
            }
        }
    }
}
