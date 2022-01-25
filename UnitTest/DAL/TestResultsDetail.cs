using DBHelper.SqlHelper;
using System;
using System.Data;
using System.Data.SqlClient;

namespace UnitTest.DAL
{
    public class TestResultsDetail
    {
        private Int64 _ID, _testResultID;
        private string _error, _ccnValTest, _affected_keys_array, _affected_key_name, _dbname;

        public TestResultsDetail()
        {
            _ID = 0;
            _testResultID = 0;
            _error = String.Empty;
            _ccnValTest = String.Empty;
            _affected_keys_array = String.Empty;
            _affected_key_name = String.Empty;
            _dbname = String.Empty;
        }

        public long ID { get => _ID; set => _ID = value; }
        public long TestResultID { get => _testResultID; set => _testResultID = value; }
        public string Error { get => _error; set => _error = value; }
        public string CcnValTest { get => _ccnValTest; set => _ccnValTest = value; }
        public string Affected_keys_array { get => _affected_keys_array; set => _affected_keys_array = value; }
        public string Affected_key_name { get => _affected_key_name; set => _affected_key_name = value; }
        public string Database_name { get => _dbname; set => _dbname = value; }

        public void insert()
        {
            string sCommand = "INSERT INTO TestResultsDetail (testResultID, affected_keys_array, affected_key_name, dbName) VALUES (" +
                "@testResultID, @affected_keys_array, @affected_key_name, @dbName)" +
                "SELECT @nID = nID FROM TestResultsDetail WHERE ID=@ID";

            SqlParameter[] param = new SqlParameter[5];

            try
            {
                param[0] = new SqlParameter("@nID", SqlDbType.BigInt);
                param[0].Direction = ParameterDirection.Output;

                param[1] = new SqlParameter("@testResultID", SqlDbType.BigInt);
                param[1].Direction = ParameterDirection.Input;
                param[1].Value = Convert.ToInt64(_testResultID);   

                param[2] = new SqlParameter("@affected_keys_array", SqlDbType.VarChar);
                param[2].Direction = ParameterDirection.Input;
                param[2].Value = Convert.ToString(_affected_keys_array);

                param[3] = new SqlParameter("@affected_key_name", SqlDbType.VarChar);
                param[3].Direction = ParameterDirection.Input;
                param[3].Value = Convert.ToString(_affected_key_name);

                param[4] = new SqlParameter("@dbName", SqlDbType.VarChar);
                param[4].Direction = ParameterDirection.Input;
                param[4].Value = Convert.ToString(_dbname);


                int ReturnValue = SqlHelper.ExecuteNonQuery(_ccnValTest, CommandType.Text, sCommand, param);
                _ID = Convert.ToInt64(param[0].Value);
            }
            catch (Exception ex)
            {
                _error = ex.ToString();
            }
        }
    }   
}
