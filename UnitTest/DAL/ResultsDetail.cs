using DBHelper.SqlHelper;
using System;
using System.Data;
using System.Data.SqlClient;

namespace UnitTest.DAL
{
    public class ResultsDetail
    {
        private Int64 _resultDetailID, _resultID;
        private string _error, _ccnValTest, _affected_keys_array, _affected_key_name;

        public ResultsDetail()
        {
            _ccnValTest = String.Empty;
            _error = String.Empty;

            _resultID = 0;
            _resultDetailID = 0;        
            _affected_keys_array = String.Empty;
            _affected_key_name = String.Empty;
           
        }

        public long ID { get => _resultDetailID; set => _resultDetailID = value; }
        public long TestResultID { get => _resultID; set => _resultID = value; }
        public string Error { get => _error; set => _error = value; }
        public string CcnValTest { get => _ccnValTest; set => _ccnValTest = value; }
        public string Affected_keys_array { get => _affected_keys_array; set => _affected_keys_array = value; }
        public string Affected_key_name { get => _affected_key_name; set => _affected_key_name = value; }
     

        public void insert()
        {
            string sCommand = "INSERT INTO ResultsDetail (resultID, affected_keys_array, affected_key_name) VALUES (" +
                "@resultID, @affected_keys_array, @affected_key_name)" +
                "SELECT @nID = nID FROM ResultsDetail WHERE ID=@ID";

            SqlParameter[] param = new SqlParameter[4];

            try
            {
                param[0] = new SqlParameter("@resultDetailID", SqlDbType.BigInt);
                param[0].Direction = ParameterDirection.Output;

                param[1] = new SqlParameter("@resultID", SqlDbType.BigInt);
                param[1].Direction = ParameterDirection.Input;
                param[1].Value = Convert.ToInt64(_resultID);   

                param[2] = new SqlParameter("@affected_keys_array", SqlDbType.VarChar);
                param[2].Direction = ParameterDirection.Input;
                param[2].Value = Convert.ToString(_affected_keys_array);

                param[3] = new SqlParameter("@affected_key_name", SqlDbType.VarChar);
                param[3].Direction = ParameterDirection.Input;
                param[3].Value = Convert.ToString(_affected_key_name);     


                int ReturnValue = SqlHelper.ExecuteNonQuery(_ccnValTest, CommandType.Text, sCommand, param);
                _resultDetailID = Convert.ToInt64(param[0].Value);
            }
            catch (Exception ex)
            {
                _error = ex.ToString();
            }
        }
    }   
}
