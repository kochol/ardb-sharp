using System;
using System.Collections.Generic;
using System.Text;

namespace ArdbSharp
{
    public struct HashEntry
    {
        private string _name;

        public string Name => _name;

        private object _value;

        public object Value => _value;

        public void SetHashEntry(string name, object value)
        {
            _name = name;
            _value = value;
        }
    }
}
