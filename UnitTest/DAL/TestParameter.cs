
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
    /// <class>TestParameter</class>
    /// </summary>
    public class TestParameter
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int64 p_paramID; 
	    protected Int64 p_testID; 
	    protected Int64 p_columnID; 
        #endregion

        #region attributes
        
        public Int64 ParamID
        {
            get => p_paramID;            
            set => p_paramID = value;               
        }
        public Int64 TestID
        {
            get => p_testID;            
            set => p_testID = value;               
        }
        public Int64 ColumnID
        {
            get => p_columnID;            
            set => p_columnID = value;               
        }
        public string Error { get => _error; set => _error = value; }
        public string Ccn { get => _ccn; set => _error = _ccn; }
        #endregion

        #region Constructor
        /// <summary>
        /// Constructor to initialize the class
        /// </summary>
        /// <param name="ccn">Conexion String to database</param>
        public TestParameter(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object TestParameter from the database 
        /// <param name="p_paramID">TestParameter primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int64 p_paramID)
        {
            String sql = "SELECT * FROM TestParameter WHERE paramID = " + Convert.ToString(p_paramID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_paramID = Convert.ToInt64(dr["paramID"]);
				p_testID = (dr["testID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["testID"]));				
				p_columnID = (dr["columnID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["columnID"]));				
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
        /// Get an TestParameter object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM TestParameter WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_paramID = Convert.ToInt64(dr["paramID"]);
				    p_testID = (dr["testID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["testID"]));				
				    p_columnID = (dr["columnID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["columnID"]));				
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
        /// Get a set of data of 'TestParameter' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = TestParameter.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'TestParameter' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM TestParameter ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "TestParameter";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'TestParameter' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = TestParameter.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'TestParameter' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM TestParameter ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "TestParameter";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'TestParameter' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = TestParameter.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'TestParameter' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM TestParameter ";
           
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
        /// Insert a new data instance into 'TestParameter' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO TestParameter("
		        + "testID,columnID) VALUES ("	       
		        + "@testID,@columnID)"
		        + " SELECT "
		        + "@paramID = paramID FROM TestParameter WHERE "
		        + "paramID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[3];            
            sqlparams[0] = new SqlParameter("@paramID", SqlDbType.BigInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@testID", SqlDbType.BigInt);	
            sqlparams[1].Value = DBNull.Value.Equals(p_testID) ? Convert.ToString(DBNull.Value) : p_testID;
            sqlparams[2] = new SqlParameter("@columnID", SqlDbType.BigInt);	
            sqlparams[2].Value = DBNull.Value.Equals(p_columnID) ? Convert.ToString(DBNull.Value) : p_columnID;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_paramID = Convert.ToInt64(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'TestParameter' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE TestParameter SET "
		        + "testID=@testID, columnID=@columnID WHERE "		  
		        + "paramID = @paramID";		
	
            SqlParameter[] sqlparams = new SqlParameter[3];
            sqlparams[0] = new SqlParameter("@paramID",SqlDbType.BigInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_paramID) ? Convert.ToInt64(DBNull.Value) : p_paramID;
            sqlparams[1] = new SqlParameter("@testID",SqlDbType.BigInt);
            sqlparams[1].Value = DBNull.Value.Equals(p_testID) ? Convert.ToInt64(DBNull.Value) : p_testID;
            sqlparams[2] = new SqlParameter("@columnID",SqlDbType.BigInt);
            sqlparams[2].Value = DBNull.Value.Equals(p_columnID) ? Convert.ToInt64(DBNull.Value) : p_columnID;
    
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
        ///  Delete an TestParameter's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM TestParameter WHERE paramID = " + p_paramID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an TestParameter's object from the database
        /// </summary>       
        /// <param name="p_paramID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int64 p_paramID)
        {
            String sql = "DELETE FROM TestParameter WHERE paramID = " + p_paramID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an TestParameter's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM TestParameter WHERE " + pWHERE;
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
