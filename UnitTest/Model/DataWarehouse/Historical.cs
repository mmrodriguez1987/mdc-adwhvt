using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DBHelper.SqlHelper;
using Tools.Files;
using Tools.DataConversion;
using System.Linq;
using Tools;
using Tools.Statistics;

namespace UnitTest.Model.DataWarehouse
{
    public class Historical
    {
        private string _ccnCDC, _ccnValTest;
        public Historical(string ccnCDC, string ccnValTest)
        {
            _ccnCDC = ccnCDC;
            _ccnValTest = ccnValTest;
        }

        public void calculateDistinctAccountID(DateTime startDate, DateTime endDate)
        {

        }
    }
}
