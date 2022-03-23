
using DBHelper.SqlHelper;
using System;
using System.Data;
using System.Data.SqlClient;

namespace UnitTest.DAL
{
    public class DB
    {
        #region system variables
        private string _error, _ccn;
        
      
        public DB(String ccn)
        {
            _ccn = ccn;
        }
        protected string Error { get => _error; set => _error = value; }
        protected string Ccn { get => _ccn; set => _ccn = value; }
      
        #endregion

        public DataSet GetObjecFromViewtDS(String viewName, String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM " + viewName;

            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try
            {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = viewName;
                return ds;
            }
            catch (Exception e)
            {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }

    }
}
