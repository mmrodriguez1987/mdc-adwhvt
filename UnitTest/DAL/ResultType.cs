
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
    /// <class>ResultType</class>
    /// </summary>
    public class ResultType
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int16 p_resultTypeID; 
	    protected String p_acronym; 
	    protected String p_resultTypeDesc; 
        #endregion

        #region attributes
        
        public Int16 ResultTypeID
        {
            get => p_resultTypeID;            
            set => p_resultTypeID = value;               
        }
        public String Acronym
        {
            get => p_acronym;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 50) {
                        throw new ArgumentOutOfRangeException("acronym", value.ToString(),
                            "Invalid value for acronym. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (50).");
                    }
                }
                p_acronym = value;
            }                           
        }
        public String ResultTypeDesc
        {
            get => p_resultTypeDesc;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 100) {
                        throw new ArgumentOutOfRangeException("resultTypeDesc", value.ToString(),
                            "Invalid value for resultTypeDesc. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (100).");
                    }
                }
                p_resultTypeDesc = value;
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
        public ResultType(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object ResultType from the database 
        /// <param name="p_resultTypeID">ResultType primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int16 p_resultTypeID)
        {
            String sql = "SELECT * FROM ResultType WHERE resultTypeID = " + Convert.ToString(p_resultTypeID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_resultTypeID = Convert.ToInt16(dr["resultTypeID"]);
				p_acronym = (dr["acronym"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["acronym"]));				
				p_resultTypeDesc = (dr["resultTypeDesc"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["resultTypeDesc"]));				
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
        /// Get an ResultType object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM ResultType WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_resultTypeID = Convert.ToInt16(dr["resultTypeID"]);
				    p_acronym = (dr["acronym"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["acronym"]));				
				    p_resultTypeDesc = (dr["resultTypeDesc"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["resultTypeDesc"]));				
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
        /// Get a set of data of 'ResultType' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = ResultType.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'ResultType' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ResultType ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "ResultType";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'ResultType' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = ResultType.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'ResultType' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ResultType ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "ResultType";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'ResultType' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = ResultType.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'ResultType' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ResultType ";
           
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
        /// Insert a new data instance into 'ResultType' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO ResultType("
		        + "acronym,resultTypeDesc) VALUES ("	       
		        + "@acronym,@resultTypeDesc)"
		        + " SELECT "
		        + "@resultTypeID = resultTypeID FROM ResultType WHERE "
		        + "resultTypeID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[3];            
            sqlparams[0] = new SqlParameter("@resultTypeID", SqlDbType.SmallInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@acronym", SqlDbType.VarChar);	
            sqlparams[1].Value = DBNull.Value.Equals(p_acronym) ? Convert.ToString(DBNull.Value) : p_acronym;
            sqlparams[2] = new SqlParameter("@resultTypeDesc", SqlDbType.VarChar);	
            sqlparams[2].Value = DBNull.Value.Equals(p_resultTypeDesc) ? Convert.ToString(DBNull.Value) : p_resultTypeDesc;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_resultTypeID = Convert.ToInt16(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'ResultType' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE ResultType SET "
		        + "acronym=@acronym, resultTypeDesc=@resultTypeDesc WHERE "		  
		        + "resultTypeID = @resultTypeID";		
	
            SqlParameter[] sqlparams = new SqlParameter[3];
            sqlparams[0] = new SqlParameter("@resultTypeID",SqlDbType.SmallInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_resultTypeID) ? Convert.ToInt16(DBNull.Value) : p_resultTypeID;
            sqlparams[1] = new SqlParameter("@acronym",SqlDbType.VarChar);
            sqlparams[1].Value = DBNull.Value.Equals(p_acronym) ? Convert.ToString(DBNull.Value) : p_acronym;
            sqlparams[2] = new SqlParameter("@resultTypeDesc",SqlDbType.VarChar);
            sqlparams[2].Value = DBNull.Value.Equals(p_resultTypeDesc) ? Convert.ToString(DBNull.Value) : p_resultTypeDesc;
    
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
        ///  Delete an ResultType's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM ResultType WHERE resultTypeID = " + p_resultTypeID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an ResultType's object from the database
        /// </summary>       
        /// <param name="p_resultTypeID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int16 p_resultTypeID)
        {
            String sql = "DELETE FROM ResultType WHERE resultTypeID = " + p_resultTypeID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an ResultType's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM ResultType WHERE " + pWHERE;
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
