using DBHelper.SqlHelper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace UnitTest.DAL
{
    public class TestResults
    {
        private Int64 _ID, _stateID, _infoID, _countCDC, _countDTW;      
        private string _error, _ccnValTest, _entity, _result, _queryCDC, _queryDTW;
        private Boolean _isActive;
        private DateTime  _iniEvalDate, _endEvalDate, _effectDate;
        

        public TestResults(string ccnValTest)
        {
            _ID = 0;
            _stateID = 0;
            _infoID = 0;
            _countCDC = -1;
            _countDTW = -1;
            _error = String.Empty;
            _ccnValTest = ccnValTest;
            _entity = String.Empty;
            _result = String.Empty;
            _queryCDC = String.Empty;
            _queryDTW = String.Empty;
            _isActive = true;
            _iniEvalDate = DateTime.MinValue;
            _endEvalDate = DateTime.MinValue;                
            _effectDate = DateTime.MinValue;
        }

        public long ID { get => _ID; set => _ID = value; }
        public long StateID { get => _stateID; set => _stateID = value; }
        public long InfoID { get => _infoID; set => _infoID = value; }
        public long CountCDC { get => _countCDC; set => _countCDC = value; }
        public long CountDTW { get => _countDTW; set => _countDTW = value; }
        public string Error { get => _error; set => _error = value; }
        public string CcnValTest { get => _ccnValTest; set => _ccnValTest = value; }
        public string Entity { get => _entity; set => _entity = value; }
        public string Result { get => _result; set => _result = value; }
        public string QueryCDC { get => _queryCDC; set => _queryCDC = value; }
        public string QueryDTW { get => _queryDTW; set => _queryDTW = value; }
        public bool IsActive { get => _isActive; set => _isActive = value; }
        public DateTime IniEvalDate { get => _iniEvalDate; set => _iniEvalDate = value; }
        public DateTime EndEvalDate { get => _endEvalDate; set => _endEvalDate = value; }
        public DateTime EffectDate { get => _effectDate; set => _effectDate = value; }

        public void insert()
        {
            string sCommand = "INSERT INTO TestResults (stateID, infoID, entity, result, iniEvalDate ,endEvalDate, countCDC, countDTW, queryCDC, queryDTW, effectDate, isActive) VALUES (" +
                "@stateID, @infoID, @entity, @result, @iniEvalDate, @endEvalDate, @countCDC, @countDTW, @queryCDC, @queryDTW, @effectDate, @isActive) " +
                "SELECT @ID = ID FROM TestResults WHERE ID=@ID";

            SqlParameter[] param = new SqlParameter[13];

            try
            {
                param[0] = new SqlParameter("@ID", SqlDbType.BigInt);
                param[0].Direction = ParameterDirection.Output;

                param[1] = new SqlParameter("@stateID", SqlDbType.BigInt);
                param[1].Direction = ParameterDirection.Input;
                param[1].Value = Convert.ToInt64(_stateID);

                param[2] = new SqlParameter("@infoID", SqlDbType.BigInt);
                param[2].Direction = ParameterDirection.Input;
                param[2].Value = Convert.ToInt64(_infoID);

                param[3] = new SqlParameter("@entity", SqlDbType.VarChar);
                param[3].Direction = ParameterDirection.Input;
                param[3].Value = Convert.ToString(_entity);

                param[4] = new SqlParameter("@result", SqlDbType.VarChar);
                param[4].Direction = ParameterDirection.Input;
                param[4].Value = Convert.ToString(_result);

                param[5] = new SqlParameter("@iniEvalDate", SqlDbType.DateTime);
                param[5].Direction = ParameterDirection.Input;
                param[5].Value = Convert.ToDateTime(_iniEvalDate);

                param[6] = new SqlParameter("@endEvalDate", SqlDbType.DateTime);
                param[6].Direction = ParameterDirection.Input;
                param[6].Value = Convert.ToDateTime(_endEvalDate);

                param[7] = new SqlParameter("@countCDC", SqlDbType.BigInt);
                param[7].Direction = ParameterDirection.Input;
                param[7].Value = Convert.ToInt64(_countCDC);

                param[8] = new SqlParameter("@countDTW", SqlDbType.BigInt);
                param[8].Direction = ParameterDirection.Input;
                param[8].Value = Convert.ToInt64(_countDTW);

                param[9] = new SqlParameter("@queryCDC", SqlDbType.VarChar);
                param[9].Direction = ParameterDirection.Input;
                param[9].Value = Convert.ToString(_queryCDC);

                param[10] = new SqlParameter("@queryDTW", SqlDbType.VarChar);
                param[10].Direction = ParameterDirection.Input;
                param[10].Value = Convert.ToString(_queryDTW);

                param[11] = new SqlParameter("@effectDate", SqlDbType.DateTime);
                param[11].Direction = ParameterDirection.Input;
                param[11].Value = Convert.ToDateTime(_effectDate);

                param[12] = new SqlParameter("@isActive", SqlDbType.Bit);
                param[12].Direction = ParameterDirection.Input;
                param[12].Value = Convert.ToInt16(_isActive);

                int ReturnValue = SqlHelper.ExecuteNonQuery(_ccnValTest, CommandType.Text, sCommand, param);
                
            }
            catch (Exception ex)
            {
                _error = ex.ToString();
            }
        }
    }
}
