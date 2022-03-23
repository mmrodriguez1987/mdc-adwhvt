
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
    /// <class>Result</class>
    /// </summary>
    public class Result
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int64 p_resultID; 
	    protected Int16 p_stateID; 
	    protected Int64 p_testID; 
	    protected String p_description; 
	    protected DateTime p_startDate; 
	    protected DateTime p_endDate; 
	    protected DateTime p_calculationDate; 
        #endregion

        #region attributes
        
        public Int64 ResultID
        {
            get => p_resultID;            
            set => p_resultID = value;               
        }
        public Int16 StateID
        {
            get => p_stateID;            
            set => p_stateID = value;               
        }
        public Int64 TestID
        {
            get => p_testID;            
            set => p_testID = value;               
        }
        public String Description
        {
            get => p_description;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 3000) {
                        throw new ArgumentOutOfRangeException("description", value.ToString(),
                            "Invalid value for description. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (3000).");
                    }
                }
                p_description = value;
            }                           
        }
        public DateTime StartDate
        {
            get => p_startDate;            
            set => p_startDate = value;               
        }
        public DateTime EndDate
        {
            get => p_endDate;            
            set => p_endDate = value;               
        }
        public DateTime CalculationDate
        {
            get => p_calculationDate;            
            set => p_calculationDate = value;               
        }
        public string Error { get => _error; set => _error = value; }
        public string Ccn { get => _ccn; set => _error = _ccn; }
        #endregion

        #region Constructor
        /// <summary>
        /// Constructor to initialize the class
        /// </summary>
        /// <param name="ccn">Conexion String to database</param>
        public Result(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object Result from the database 
        /// <param name="p_resultID">Result primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int64 p_resultID)
        {
            String sql = "SELECT * FROM Result WHERE resultID = " + Convert.ToString(p_resultID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_resultID = Convert.ToInt64(dr["resultID"]);
				p_stateID = (dr["stateID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["stateID"]));				
				p_testID = (dr["testID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["testID"]));				
				p_description = (dr["description"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["description"]));				
				p_startDate = (dr["startDate"] == System.DBNull.Value) ? (Convert.ToDateTime(null)) : (Convert.ToDateTime(dr["startDate"]));				
				p_endDate = (dr["endDate"] == System.DBNull.Value) ? (Convert.ToDateTime(null)) : (Convert.ToDateTime(dr["endDate"]));				
				p_calculationDate = (dr["calculationDate"] == System.DBNull.Value) ? (Convert.ToDateTime(null)) : (Convert.ToDateTime(dr["calculationDate"]));				
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
        /// Get an Result object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM Result WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_resultID = Convert.ToInt64(dr["resultID"]);
				    p_stateID = (dr["stateID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["stateID"]));				
				    p_testID = (dr["testID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["testID"]));				
				    p_description = (dr["description"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["description"]));				
				    p_startDate = (dr["startDate"] == System.DBNull.Value) ? (Convert.ToDateTime(null)) : (Convert.ToDateTime(dr["startDate"]));				
				    p_endDate = (dr["endDate"] == System.DBNull.Value) ? (Convert.ToDateTime(null)) : (Convert.ToDateTime(dr["endDate"]));				
				    p_calculationDate = (dr["calculationDate"] == System.DBNull.Value) ? (Convert.ToDateTime(null)) : (Convert.ToDateTime(dr["calculationDate"]));				
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
        /// Get a set of data of 'Result' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = Result.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'Result' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM Result ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "Result";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'Result' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = Result.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'Result' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM Result ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "Result";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'Result' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = Result.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'Result' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM Result ";
           
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
        /// Insert a new data instance into 'Result' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO Result("
		        + "stateID,testID,description,startDate,endDate,calculationDate) VALUES ("	       
		        + "@stateID,@testID,@description,@startDate,@endDate,@calculationDate)"
		        + " SELECT "
		        + "@resultID = resultID FROM Result WHERE "
		        + "resultID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[7];            
            sqlparams[0] = new SqlParameter("@resultID", SqlDbType.BigInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@stateID", SqlDbType.SmallInt);	
            sqlparams[1].Value = DBNull.Value.Equals(p_stateID) ? Convert.ToString(DBNull.Value) : p_stateID;
            sqlparams[2] = new SqlParameter("@testID", SqlDbType.BigInt);	
            sqlparams[2].Value = DBNull.Value.Equals(p_testID) ? Convert.ToString(DBNull.Value) : p_testID;
            sqlparams[3] = new SqlParameter("@description", SqlDbType.VarChar);	
            sqlparams[3].Value = DBNull.Value.Equals(p_description) ? Convert.ToString(DBNull.Value) : p_description;
            sqlparams[4] = new SqlParameter("@startDate", SqlDbType.DateTime);	
            sqlparams[4].Value = DBNull.Value.Equals(p_startDate) ? Convert.ToString(DBNull.Value) : p_startDate;
            sqlparams[5] = new SqlParameter("@endDate", SqlDbType.DateTime);	
            sqlparams[5].Value = DBNull.Value.Equals(p_endDate) ? Convert.ToString(DBNull.Value) : p_endDate;
            sqlparams[6] = new SqlParameter("@calculationDate", SqlDbType.DateTime);	
            sqlparams[6].Value = DBNull.Value.Equals(p_calculationDate) ? Convert.ToString(DBNull.Value) : p_calculationDate;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_resultID = Convert.ToInt64(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'Result' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE Result SET "
		        + "stateID=@stateID, testID=@testID, description=@description, startDate=@startDate, endDate=@endDate, calculationDate=@calculationDate WHERE "		  
		        + "resultID = @resultID";		
	
            SqlParameter[] sqlparams = new SqlParameter[7];
            sqlparams[0] = new SqlParameter("@resultID",SqlDbType.BigInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_resultID) ? Convert.ToInt64(DBNull.Value) : p_resultID;
            sqlparams[1] = new SqlParameter("@stateID",SqlDbType.SmallInt);
            sqlparams[1].Value = DBNull.Value.Equals(p_stateID) ? Convert.ToInt16(DBNull.Value) : p_stateID;
            sqlparams[2] = new SqlParameter("@testID",SqlDbType.BigInt);
            sqlparams[2].Value = DBNull.Value.Equals(p_testID) ? Convert.ToInt64(DBNull.Value) : p_testID;
            sqlparams[3] = new SqlParameter("@description",SqlDbType.VarChar);
            sqlparams[3].Value = DBNull.Value.Equals(p_description) ? Convert.ToString(DBNull.Value) : p_description;
            sqlparams[4] = new SqlParameter("@startDate",SqlDbType.DateTime);
            sqlparams[4].Value = DBNull.Value.Equals(p_startDate) ? Convert.ToDateTime(DBNull.Value) : p_startDate;
            sqlparams[5] = new SqlParameter("@endDate",SqlDbType.DateTime);
            sqlparams[5].Value = DBNull.Value.Equals(p_endDate) ? Convert.ToDateTime(DBNull.Value) : p_endDate;
            sqlparams[6] = new SqlParameter("@calculationDate",SqlDbType.DateTime);
            sqlparams[6].Value = DBNull.Value.Equals(p_calculationDate) ? Convert.ToDateTime(DBNull.Value) : p_calculationDate;
    
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
        ///  Delete an Result's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM Result WHERE resultID = " + p_resultID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an Result's object from the database
        /// </summary>       
        /// <param name="p_resultID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int64 p_resultID)
        {
            String sql = "DELETE FROM Result WHERE resultID = " + p_resultID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an Result's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM Result WHERE " + pWHERE;
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
