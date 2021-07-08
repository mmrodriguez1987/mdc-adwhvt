using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace UnitTest
{
    public class Business
    {
        private IConfiguration _conf { get; }
        public Business(IConfiguration configuration)
        {
            _conf = configuration;
        }

        public static Int64 CountRowsCCB ()
        {
            return 0;
        }

        public static Int64 CountRowsDTW() 
        {
            return 0;
        }

    }
}
