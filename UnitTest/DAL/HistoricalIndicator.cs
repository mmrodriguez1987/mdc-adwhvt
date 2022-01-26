using DBHelper.SqlHelper;
using System;
using System.Data;
using System.Data.SqlClient;

namespace UnitTest.DAL
{
    public class HistoricalIndicator
    {
        private Int64 _histIndicatorID;
        private Int64 _columnID;
        private Int64 _distinctCountVal;
        private Int64 _newCountVal;
        private Int64 _updatedCountVal;
        private Int64 _maxVal;
        private Int64 _minVal;
        private DateTime _calculatedDate;
        private Boolean _isActive;
        private string _error;
        private string _ccn;


        public HistoricalIndicator(string ccn)
        {
            _ccn = ccn;
            _error = String.Empty;
            
            _histIndicatorID = 0;
            _columnID = 0;
            _distinctCountVal = -1;
            _newCountVal = -1;
            _updatedCountVal = -1;
            _maxVal = -1;
            _minVal = -1;
            _calculatedDate = DateTime.MinValue;
            _isActive = true;
            
        }

        public long ID { get => _histIndicatorID; set => _histIndicatorID = value; }
        public long ColumnID { get => _columnID; set => _columnID = value; }
        public long DistinctCountVal { get => _distinctCountVal; set => _distinctCountVal = value; }
        public long NewCountVal { get => _newCountVal; set => _newCountVal = value; }
        public long UpdatedCountVal { get => _updatedCountVal; set => _updatedCountVal = value; }
        public long MaxVal { get => _maxVal; set => _maxVal = value; }
        public long MinVal { get => _minVal; set => _minVal = value; }
        public DateTime CalculatedDate { get => _calculatedDate; set => _calculatedDate = value; }
        public bool IsActive { get => _isActive; set => _isActive = value; }
        public string Error { get => _error; set => _error = value; }

        public void insert()
        {

            string sCommand = "INSERT INTO HistoricalIndicator(" +
                "columnID, distinctCountVal, newCountVal, updatedCountVal, maxVal, minVal, calculatedDate, isActive) VALUES " +
                "(@columnID, @distinctCountVal, @newCountVal, @updatedCountVal, @maxVal, @minVal, @calculatedDate, @isActive) " +
                "SELECT @histIndicator = histIndicator FROM HistoricalIndicator WHERE histIndicator=@histIndicator";
            
            SqlParameter[] param = new SqlParameter[9];

            try
            {
                param[0] = new SqlParameter("@histIndicator", SqlDbType.BigInt);
                param[0].Direction = ParameterDirection.Output;

                param[1] = new SqlParameter("@columnID", SqlDbType.BigInt);
                param[1].Direction = ParameterDirection.Input;
                param[1].Value = Convert.ToInt64(_columnID);

                param[2] = new SqlParameter("@distinctCountVal", SqlDbType.BigInt);
                param[2].Direction = ParameterDirection.Input;
                param[2].Value = Convert.ToInt64(_distinctCountVal);

                param[3] = new SqlParameter("@newCountVal", SqlDbType.BigInt);
                param[3].Direction = ParameterDirection.Input;
                param[3].Value = Convert.ToInt64(_newCountVal);

                param[4] = new SqlParameter("@updatedCountVal", SqlDbType.BigInt);
                param[4].Direction = ParameterDirection.Input;
                param[4].Value = Convert.ToInt64(_updatedCountVal);

                param[5] = new SqlParameter("@maxVal", SqlDbType.BigInt);
                param[5].Direction = ParameterDirection.Input;
                param[5].Value = Convert.ToInt64(_maxVal);

                param[6] = new SqlParameter("@minVal", SqlDbType.BigInt);
                param[6].Direction = ParameterDirection.Input;
                param[6].Value = Convert.ToInt64(_minVal);

                param[7] = new SqlParameter("@calculatedDate", SqlDbType.DateTime);
                param[7].Direction = ParameterDirection.Input;
                param[7].Value = Convert.ToDateTime(_calculatedDate);

                param[8] = new SqlParameter("@isActive", SqlDbType.Bit);
                param[8].Direction = ParameterDirection.Input;
                param[8].Value = Convert.ToBoolean(_isActive);

                int ReturnValue = SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, param);
                
            }
            catch (Exception ex)
            {
                _error = ex.ToString();
            }
        }
    }
}
