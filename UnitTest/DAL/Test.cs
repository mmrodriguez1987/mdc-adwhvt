
using DBHelper.SqlHelper;
using System;
using System.Data;
using System.Data.SqlClient;

/// <summary>
/// Data Access Layer
/// Project: Data Validations, DTWVAL
/// Company: Miami Dade County, MDC
/// <author>Michael Rodriguez</author>
/// </summary>
namespace UnitTest.DAL
{
    /// <summary>  
    /// <class>Test</class>
    /// </summary>
    public class Test
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int64 p_testID; 
	    protected Int16 p_testTypeID; 
	    protected String p_testName; 
	    protected String p_testDescription; 
	    protected String p_query; 
	    protected Boolean p_active; 
        #endregion

        #region attributes
        
        public Int64 TestID
        {
            get => p_testID;            
            set => p_testID = value;               
        }
        public Int16 TestTypeID
        {
            get => p_testTypeID;            
            set => p_testTypeID = value;               
        }
        public String TestName
        {
            get => p_testName;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 500) {
                        throw new ArgumentOutOfRangeException("testName", value.ToString(),
                            "Invalid value for testName. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (500).");
                    }
                }
                p_testName = value;
            }                           
        }
        public String TestDescription
        {
            get => p_testDescription;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 500) {
                        throw new ArgumentOutOfRangeException("testDescription", value.ToString(),
                            "Invalid value for testDescription. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (500).");
                    }
                }
                p_testDescription = value;
            }                           
        }
        public String Query
        {
            get => p_query;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 3000) {
                        throw new ArgumentOutOfRangeException("query", value.ToString(),
                            "Invalid value for query. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (3000).");
                    }
                }
                p_query = value;
            }                           
        }
        public Boolean Active
        {
            get => p_active;            
            set => p_active = value;               
        }
        public string Error { get => _error; set => _error = value; }
        public string Ccn { get => _ccn; set => _error = _ccn; }
        #endregion

        #region Constructor
        /// <summary>
        /// Constructor to initialize the class
        /// </summary>
        /// <param name="ccn">Conexion String to database</param>
        public Test(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object Test from the database 
        /// <param name="p_testID">Test primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int64 p_testID)
        {
            String sql = "SELECT * FROM Test WHERE testID = " + Convert.ToString(p_testID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_testID = Convert.ToInt64(dr["testID"]);
				p_testTypeID = (dr["testTypeID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["testTypeID"]));				
				p_testName = (dr["testName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["testName"]));				
				p_testDescription = (dr["testDescription"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["testDescription"]));				
				p_query = (dr["query"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["query"]));				
				p_active = (dr["active"] == System.DBNull.Value) ? (Convert.ToBoolean(null)) : (Convert.ToBoolean(dr["active"]));				
                    return true;
                } else { return false; }
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
            finally {
                if (dr != null) {
                    if (!dr.IsClosed) { dr.Close(); }
                    dr = null;
                }
            }
        }
        
        /// <summary>
        /// Get an Test object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM Test WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_testID = Convert.ToInt64(dr["testID"]);
				    p_testTypeID = (dr["testTypeID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["testTypeID"]));				
				    p_testName = (dr["testName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["testName"]));				
				    p_testDescription = (dr["testDescription"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["testDescription"]));				
				    p_query = (dr["query"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["query"]));				
				    p_active = (dr["active"] == System.DBNull.Value) ? (Convert.ToBoolean(null)) : (Convert.ToBoolean(dr["active"]));				
                    return true;
                } else {
                    return false;
                }
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
            finally {
                if (dr != null) {
                    if (!dr.IsClosed) {
                        dr.Close();
                    }
                    dr = null;
                }
            }
        }
        
        /// <summary>
        /// Get a set of data of 'Test' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = Test.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'Test' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM Test ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "Test";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'Test' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = Test.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'Test' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM Test ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "Test";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'Test' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = Test.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'Test' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM Test ";
           
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : " ";           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : " ";

            try {
                return SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        #endregion 
        
        #region Insert Methods
        /// <summary>
        /// Insert a new data instance into 'Test' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO Test("
		        + "testTypeID,testName,testDescription,query,active) VALUES ("	       
		        + "@testTypeID,@testName,@testDescription,@query,@active)"
		        + " SELECT "
		        + "@testID = testID FROM Test WHERE "
		        + "testID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[6];            
            sqlparams[0] = new SqlParameter("@testID", SqlDbType.BigInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@testTypeID", SqlDbType.SmallInt);	
            sqlparams[1].Value = DBNull.Value.Equals(p_testTypeID) ? Convert.ToString(DBNull.Value) : p_testTypeID;
            sqlparams[2] = new SqlParameter("@testName", SqlDbType.VarChar);	
            sqlparams[2].Value = DBNull.Value.Equals(p_testName) ? Convert.ToString(DBNull.Value) : p_testName;
            sqlparams[3] = new SqlParameter("@testDescription", SqlDbType.VarChar);	
            sqlparams[3].Value = DBNull.Value.Equals(p_testDescription) ? Convert.ToString(DBNull.Value) : p_testDescription;
            sqlparams[4] = new SqlParameter("@query", SqlDbType.VarChar);	
            sqlparams[4].Value = DBNull.Value.Equals(p_query) ? Convert.ToString(DBNull.Value) : p_query;
            sqlparams[5] = new SqlParameter("@active", SqlDbType.Bit);	
            sqlparams[5].Value = DBNull.Value.Equals(p_active) ? Convert.ToString(DBNull.Value) : p_active;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_testID = Convert.ToInt64(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'Test' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE Test SET "
		        + "testTypeID=@testTypeID, testName=@testName, testDescription=@testDescription, query=@query, active=@active WHERE "		  
		        + "testID = @testID";		
	
            SqlParameter[] sqlparams = new SqlParameter[6];
            sqlparams[0] = new SqlParameter("@testID",SqlDbType.BigInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_testID) ? Convert.ToInt64(DBNull.Value) : p_testID;
            sqlparams[1] = new SqlParameter("@testTypeID",SqlDbType.SmallInt);
            sqlparams[1].Value = DBNull.Value.Equals(p_testTypeID) ? Convert.ToInt16(DBNull.Value) : p_testTypeID;
            sqlparams[2] = new SqlParameter("@testName",SqlDbType.VarChar);
            sqlparams[2].Value = DBNull.Value.Equals(p_testName) ? Convert.ToString(DBNull.Value) : p_testName;
            sqlparams[3] = new SqlParameter("@testDescription",SqlDbType.VarChar);
            sqlparams[3].Value = DBNull.Value.Equals(p_testDescription) ? Convert.ToString(DBNull.Value) : p_testDescription;
            sqlparams[4] = new SqlParameter("@query",SqlDbType.VarChar);
            sqlparams[4].Value = DBNull.Value.Equals(p_query) ? Convert.ToString(DBNull.Value) : p_query;
            sqlparams[5] = new SqlParameter("@active",SqlDbType.Bit);
            sqlparams[5].Value = DBNull.Value.Equals(p_active) ? Convert.ToBoolean(DBNull.Value) : p_active;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        #endregion
        
        #region Delete Methods

        /// <summary>
        ///  Delete an Test's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM Test WHERE testID = " + p_testID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an Test's object from the database
        /// </summary>       
        /// <param name="p_testID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int64 p_testID)
        {
            String sql = "DELETE FROM Test WHERE testID = " + p_testID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an Test's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM Test WHERE " + pWHERE;
            try {                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        #endregion

    }
}
