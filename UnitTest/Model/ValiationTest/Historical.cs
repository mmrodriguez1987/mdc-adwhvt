using System;
using UnitTest.DAL;

namespace UnitTest.Model.ValidationTest
{
    public class Historical
    {
        private string _ccnCDC, _ccnValTest, _error;
        private HistoricalIndicator _historicalIndicator;
        public Historical(string ccnValTest)
        {            
            _ccnValTest = ccnValTest;
            Error = String.Empty;
        }

        public Historical(string ccnValTest, string ccnCDC)
        {
            _ccnCDC = ccnCDC;
            _ccnValTest = ccnValTest;
            _error = String.Empty;
        }

        public string Error { get => _error; set => _error = value; }
        public string CcnValTest { get => _ccnValTest; set => _ccnValTest = value; }
        public string CcnCDC { get => _ccnCDC; set => _ccnCDC = value; }

        public void calculateDistinctAccountID(DateTime startDate, DateTime endDate)
        {

        }
        /// <summary>
        /// Record a Historical Indicator
        /// </summary>
        /// <param name="columnID">ColumnID Evaluated</param>
        /// <param name="indicatorType">Idicator Type: Distinct, New, Updated, Max, Min</param>
        /// <param name="count">Evaluated result</param>
        /// <param name="calculatedDate">Evaluated Date</param>
        public void recordHistorical(Int64 columnID, Int16 indicatorType, Double count, DateTime calculatedDate)
        {
            _historicalIndicator = new HistoricalIndicator(_ccnValTest);
            _historicalIndicator.ColumnID = columnID;
            _historicalIndicator.CalculatedDate = calculatedDate;   
            _historicalIndicator.IndicatorTypeID = indicatorType;
            _historicalIndicator.Count = count;
            _historicalIndicator.Insert();

            _error = (!String.IsNullOrEmpty(_historicalIndicator.Error)) ? _historicalIndicator.Error : _error;
           
        }
    }
}
