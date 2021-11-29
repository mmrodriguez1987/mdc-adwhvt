using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace UnitTest.Model
{
    public class Native
    {
        private String _cnn, _field, _table, _condition;

        public Native(string cnn)
        {
            Cnn = cnn;
        }

        public string Cnn { get => _cnn; set => _cnn = value; }
        public string Field { get => _field; set => _field = value; }
        public string Table { get => _table; set => _table = value; }
        public string Condition { get => _condition; set => _condition = value; }
    }
}
