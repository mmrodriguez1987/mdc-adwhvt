
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
    /// <class>ResultDetail</class>
    /// </summary>
    public class ResultDetail
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int64 p_resultDetailID; 
	    protected Int64 p_resultID; 
	    protected Int16 p_resultTypeID; 
	    protected Double p_count; 
	    protected String p_affectedIDs; 
	    protected String p_affectedDesc; 
        #endregion

        #region attributes
        
        public Int64 ResultDetailID
        {
            get => p_resultDetailID;            
            set => p_resultDetailID = value;               
        }
        public Int64 ResultID
        {
            get => p_resultID;            
            set => p_resultID = value;               
        }
        public Int16 ResultTypeID
        {
            get => p_resultTypeID;            
            set => p_resultTypeID = value;               
        }
        public Double Count
        {
            get => p_count;            
            set => p_count = value;               
        }
        public String AffectedIDs
        {
            get => p_affectedIDs;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 5000) {
                        throw new ArgumentOutOfRangeException("affectedIDs", value.ToString(),
                            "Invalid value for affectedIDs. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (5000).");
                    }
                }
                p_affectedIDs = value;
            }                           
        }
        public String AffectedDesc
        {
            get => p_affectedDesc;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 100) {
                        throw new ArgumentOutOfRangeException("affectedDesc", value.ToString(),
                            "Invalid value for affectedDesc. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (100).");
                    }
                }
                p_affectedDesc = value;
            }                           
        }
        public string Error { get => _error; set => _error = value; }
        public string Ccn { get => _ccn; set => _error = _ccn; }
        #endregion

        #region Constructor
        /// <summary>
        /// Constructor to initialize the class
        /// </summary>
        /// <param name="ccn">Conexion String to database</param>
        public ResultDetail(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object ResultDetail from the database 
        /// <param name="p_resultDetailID">ResultDetail primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int64 p_resultDetailID)
        {
            String sql = "SELECT * FROM ResultDetail WHERE resultDetailID = " + Convert.ToString(p_resultDetailID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_resultDetailID = Convert.ToInt64(dr["resultDetailID"]);
				p_resultID = (dr["resultID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["resultID"]));				
				p_resultTypeID = (dr["resultTypeID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["resultTypeID"]));				
				p_count = (dr["count"] == System.DBNull.Value) ? (Convert.ToDouble(null)) : (Convert.ToDouble(dr["count"]));				
				p_affectedIDs = (dr["affectedIDs"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["affectedIDs"]));				
				p_affectedDesc = (dr["affectedDesc"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["affectedDesc"]));				
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
        /// Get an ResultDetail object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM ResultDetail WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_resultDetailID = Convert.ToInt64(dr["resultDetailID"]);
				    p_resultID = (dr["resultID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["resultID"]));				
				    p_resultTypeID = (dr["resultTypeID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["resultTypeID"]));				
				    p_count = (dr["count"] == System.DBNull.Value) ? (Convert.ToDouble(null)) : (Convert.ToDouble(dr["count"]));				
				    p_affectedIDs = (dr["affectedIDs"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["affectedIDs"]));				
				    p_affectedDesc = (dr["affectedDesc"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["affectedDesc"]));				
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
        /// Get a set of data of 'ResultDetail' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = ResultDetail.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'ResultDetail' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ResultDetail ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "ResultDetail";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'ResultDetail' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = ResultDetail.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'ResultDetail' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ResultDetail ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "ResultDetail";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'ResultDetail' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = ResultDetail.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'ResultDetail' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ResultDetail ";
           
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
        /// Insert a new data instance into 'ResultDetail' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO ResultDetail("
		        + "resultID,resultTypeID,count,affectedIDs,affectedDesc) VALUES ("	       
		        + "@resultID,@resultTypeID,@count,@affectedIDs,@affectedDesc)"
		        + " SELECT "
		        + "@resultDetailID = resultDetailID FROM ResultDetail WHERE "
		        + "resultDetailID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[6];            
            sqlparams[0] = new SqlParameter("@resultDetailID", SqlDbType.BigInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@resultID", SqlDbType.BigInt);	
            sqlparams[1].Value = DBNull.Value.Equals(p_resultID) ? Convert.ToString(DBNull.Value) : p_resultID;
            sqlparams[2] = new SqlParameter("@resultTypeID", SqlDbType.SmallInt);	
            sqlparams[2].Value = DBNull.Value.Equals(p_resultTypeID) ? Convert.ToString(DBNull.Value) : p_resultTypeID;
            sqlparams[3] = new SqlParameter("@count", SqlDbType.Float);	
            sqlparams[3].Value = DBNull.Value.Equals(p_count) ? Convert.ToString(DBNull.Value) : p_count;
            sqlparams[4] = new SqlParameter("@affectedIDs", SqlDbType.VarChar);	
            sqlparams[4].Value = String.IsNullOrEmpty(p_affectedIDs) ? DBNull.Value : p_affectedIDs;
            sqlparams[5] = new SqlParameter("@affectedDesc", SqlDbType.VarChar);	
            sqlparams[5].Value = String.IsNullOrEmpty(p_affectedDesc) ? DBNull.Value : p_affectedDesc;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_resultDetailID = Convert.ToInt64(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'ResultDetail' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE ResultDetail SET "
		        + "resultID=@resultID, resultTypeID=@resultTypeID, count=@count, affectedIDs=@affectedIDs, affectedDesc=@affectedDesc WHERE "		  
		        + "resultDetailID = @resultDetailID";		
	
            SqlParameter[] sqlparams = new SqlParameter[6];
            sqlparams[0] = new SqlParameter("@resultDetailID",SqlDbType.BigInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_resultDetailID) ? Convert.ToInt64(DBNull.Value) : p_resultDetailID;
            sqlparams[1] = new SqlParameter("@resultID",SqlDbType.BigInt);
            sqlparams[1].Value = DBNull.Value.Equals(p_resultID) ? Convert.ToInt64(DBNull.Value) : p_resultID;
            sqlparams[2] = new SqlParameter("@resultTypeID",SqlDbType.SmallInt);
            sqlparams[2].Value = DBNull.Value.Equals(p_resultTypeID) ? Convert.ToInt16(DBNull.Value) : p_resultTypeID;
            sqlparams[3] = new SqlParameter("@count",SqlDbType.Float);
            sqlparams[3].Value = DBNull.Value.Equals(p_count) ? Convert.ToDouble(DBNull.Value) : p_count;
            sqlparams[4] = new SqlParameter("@affectedIDs",SqlDbType.VarChar);
            sqlparams[4].Value = DBNull.Value.Equals(p_affectedIDs) ? Convert.ToString(DBNull.Value) : p_affectedIDs;
            sqlparams[5] = new SqlParameter("@affectedDesc",SqlDbType.VarChar);
            sqlparams[5].Value = DBNull.Value.Equals(p_affectedDesc) ? Convert.ToString(DBNull.Value) : p_affectedDesc;
    
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
        ///  Delete an ResultDetail's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM ResultDetail WHERE resultDetailID = " + p_resultDetailID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an ResultDetail's object from the database
        /// </summary>       
        /// <param name="p_resultDetailID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int64 p_resultDetailID)
        {
            String sql = "DELETE FROM ResultDetail WHERE resultDetailID = " + p_resultDetailID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an ResultDetail's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM ResultDetail WHERE " + pWHERE;
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
