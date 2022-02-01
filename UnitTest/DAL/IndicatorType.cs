
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
    /// <class>IndicatorType</class>
    /// </summary>
    public class IndicatorType
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int16 p_indicatorTypeID; 
	    protected String p_typeDesc; 
	    protected Boolean p_active; 
        #endregion

        #region attributes
        
        public Int16 IndicatorTypeID
        {
            get => p_indicatorTypeID;            
            set => p_indicatorTypeID = value;               
        }
        public String TypeDesc
        {
            get => p_typeDesc;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 100) {
                        throw new ArgumentOutOfRangeException("typeDesc", value.ToString(),
                            "Invalid value for typeDesc. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (100).");
                    }
                }
                p_typeDesc = value;
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
        public IndicatorType(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object IndicatorType from the database 
        /// <param name="p_indicatorTypeID">IndicatorType primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int16 p_indicatorTypeID)
        {
            String sql = "SELECT * FROM IndicatorType WHERE indicatorTypeID = " + Convert.ToString(p_indicatorTypeID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_indicatorTypeID = Convert.ToInt16(dr["indicatorTypeID"]);
				p_typeDesc = (dr["typeDesc"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["typeDesc"]));				
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
        /// Get an IndicatorType object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM IndicatorType WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_indicatorTypeID = Convert.ToInt16(dr["indicatorTypeID"]);
				    p_typeDesc = (dr["typeDesc"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["typeDesc"]));				
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
        /// Get a set of data of 'IndicatorType' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = IndicatorType.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'IndicatorType' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM IndicatorType ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "IndicatorType";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'IndicatorType' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = IndicatorType.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'IndicatorType' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM IndicatorType ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "IndicatorType";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'IndicatorType' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = IndicatorType.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'IndicatorType' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM IndicatorType ";
           
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
        /// Insert a new data instance into 'IndicatorType' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO IndicatorType("
		        + "typeDesc,active) VALUES ("	       
		        + "@typeDesc,@active)"
		        + " SELECT "
		        + "@indicatorTypeID = indicatorTypeID FROM IndicatorType WHERE "
		        + "indicatorTypeID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[3];            
            sqlparams[0] = new SqlParameter("@indicatorTypeID", SqlDbType.SmallInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@typeDesc", SqlDbType.VarChar);	
            sqlparams[1].Value = DBNull.Value.Equals(p_typeDesc) ? Convert.ToString(DBNull.Value) : p_typeDesc;
            sqlparams[2] = new SqlParameter("@active", SqlDbType.Bit);	
            sqlparams[2].Value = DBNull.Value.Equals(p_active) ? Convert.ToString(DBNull.Value) : p_active;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_indicatorTypeID = Convert.ToInt16(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'IndicatorType' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE IndicatorType SET "
		        + "typeDesc=@typeDesc, active=@active WHERE "		  
		        + "indicatorTypeID = @indicatorTypeID";		
	
            SqlParameter[] sqlparams = new SqlParameter[3];
            sqlparams[0] = new SqlParameter("@indicatorTypeID",SqlDbType.SmallInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_indicatorTypeID) ? Convert.ToInt16(DBNull.Value) : p_indicatorTypeID;
            sqlparams[1] = new SqlParameter("@typeDesc",SqlDbType.VarChar);
            sqlparams[1].Value = DBNull.Value.Equals(p_typeDesc) ? Convert.ToString(DBNull.Value) : p_typeDesc;
            sqlparams[2] = new SqlParameter("@active",SqlDbType.Bit);
            sqlparams[2].Value = DBNull.Value.Equals(p_active) ? Convert.ToBoolean(DBNull.Value) : p_active;
    
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
        ///  Delete an IndicatorType's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM IndicatorType WHERE indicatorTypeID = " + p_indicatorTypeID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an IndicatorType's object from the database
        /// </summary>       
        /// <param name="p_indicatorTypeID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int16 p_indicatorTypeID)
        {
            String sql = "DELETE FROM IndicatorType WHERE indicatorTypeID = " + p_indicatorTypeID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an IndicatorType's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM IndicatorType WHERE " + pWHERE;
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
