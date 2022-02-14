
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
    /// <class>ResultStat</class>
    /// </summary>
    public class ResultStat
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int64 p_resultStatID; 
	    protected Int64 p_columnID; 
	    protected Int16 p_indicatorTypeID; 
	    protected Double p_count; 
	    protected DateTime p_calculatedDate; 
        #endregion

        #region attributes
        
        public Int64 ResultStatID
        {
            get => p_resultStatID;            
            set => p_resultStatID = value;               
        }
        public Int64 ColumnID
        {
            get => p_columnID;            
            set => p_columnID = value;               
        }
        public Int16 IndicatorTypeID
        {
            get => p_indicatorTypeID;            
            set => p_indicatorTypeID = value;               
        }
        public Double Count
        {
            get => p_count;            
            set => p_count = value;               
        }
        public DateTime CalculatedDate
        {
            get => p_calculatedDate;            
            set => p_calculatedDate = value;               
        }
        public string Error { get => _error; set => _error = value; }
        public string Ccn { get => _ccn; set => _error = _ccn; }
        #endregion

        #region Constructor
        /// <summary>
        /// Constructor to initialize the class
        /// </summary>
        /// <param name="ccn">Conexion String to database</param>
        public ResultStat(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object ResultStat from the database 
        /// <param name="p_resultStatID">ResultStat primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int64 p_resultStatID)
        {
            String sql = "SELECT * FROM ResultStat WHERE resultStatID = " + Convert.ToString(p_resultStatID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_resultStatID = Convert.ToInt64(dr["resultStatID"]);
				p_columnID = (dr["columnID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["columnID"]));				
				p_indicatorTypeID = (dr["indicatorTypeID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["indicatorTypeID"]));				
				p_count = (dr["count"] == System.DBNull.Value) ? (Convert.ToDouble(null)) : (Convert.ToDouble(dr["count"]));				
				p_calculatedDate = (dr["calculatedDate"] == System.DBNull.Value) ? (Convert.ToDateTime(null)) : (Convert.ToDateTime(dr["calculatedDate"]));				
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
        /// Get an ResultStat object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM ResultStat WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_resultStatID = Convert.ToInt64(dr["resultStatID"]);
				    p_columnID = (dr["columnID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["columnID"]));				
				    p_indicatorTypeID = (dr["indicatorTypeID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["indicatorTypeID"]));				
				    p_count = (dr["count"] == System.DBNull.Value) ? (Convert.ToDouble(null)) : (Convert.ToDouble(dr["count"]));				
				    p_calculatedDate = (dr["calculatedDate"] == System.DBNull.Value) ? (Convert.ToDateTime(null)) : (Convert.ToDateTime(dr["calculatedDate"]));				
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
        /// Get a set of data of 'ResultStat' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = ResultStat.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'ResultStat' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ResultStat ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "ResultStat";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'ResultStat' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = ResultStat.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'ResultStat' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ResultStat ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "ResultStat";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'ResultStat' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = ResultStat.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'ResultStat' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ResultStat ";
           
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
        /// Insert a new data instance into 'ResultStat' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO ResultStat("
		        + "columnID,indicatorTypeID,count,calculatedDate) VALUES ("	       
		        + "@columnID,@indicatorTypeID,@count,@calculatedDate)"
		        + " SELECT "
		        + "@resultStatID = resultStatID FROM ResultStat WHERE "
		        + "resultStatID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[5];            
            sqlparams[0] = new SqlParameter("@resultStatID", SqlDbType.BigInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@columnID", SqlDbType.BigInt);	
            sqlparams[1].Value = DBNull.Value.Equals(p_columnID) ? Convert.ToString(DBNull.Value) : p_columnID;
            sqlparams[2] = new SqlParameter("@indicatorTypeID", SqlDbType.SmallInt);	
            sqlparams[2].Value = DBNull.Value.Equals(p_indicatorTypeID) ? Convert.ToString(DBNull.Value) : p_indicatorTypeID;
            sqlparams[3] = new SqlParameter("@count", SqlDbType.Float);	
            sqlparams[3].Value = DBNull.Value.Equals(p_count) ? Convert.ToString(DBNull.Value) : p_count;
            sqlparams[4] = new SqlParameter("@calculatedDate", SqlDbType.DateTime);	
            sqlparams[4].Value = DBNull.Value.Equals(p_calculatedDate) ? Convert.ToString(DBNull.Value) : p_calculatedDate;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_resultStatID = Convert.ToInt64(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'ResultStat' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE ResultStat SET "
		        + "columnID=@columnID, indicatorTypeID=@indicatorTypeID, count=@count, calculatedDate=@calculatedDate WHERE "		  
		        + "resultStatID = @resultStatID";		
	
            SqlParameter[] sqlparams = new SqlParameter[5];
            sqlparams[0] = new SqlParameter("@resultStatID",SqlDbType.BigInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_resultStatID) ? Convert.ToInt64(DBNull.Value) : p_resultStatID;
            sqlparams[1] = new SqlParameter("@columnID",SqlDbType.BigInt);
            sqlparams[1].Value = DBNull.Value.Equals(p_columnID) ? Convert.ToInt64(DBNull.Value) : p_columnID;
            sqlparams[2] = new SqlParameter("@indicatorTypeID",SqlDbType.SmallInt);
            sqlparams[2].Value = DBNull.Value.Equals(p_indicatorTypeID) ? Convert.ToInt16(DBNull.Value) : p_indicatorTypeID;
            sqlparams[3] = new SqlParameter("@count",SqlDbType.Float);
            sqlparams[3].Value = DBNull.Value.Equals(p_count) ? Convert.ToDouble(DBNull.Value) : p_count;
            sqlparams[4] = new SqlParameter("@calculatedDate",SqlDbType.DateTime);
            sqlparams[4].Value = DBNull.Value.Equals(p_calculatedDate) ? Convert.ToDateTime(DBNull.Value) : p_calculatedDate;
    
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
        ///  Delete an ResultStat's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM ResultStat WHERE resultStatID = " + p_resultStatID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an ResultStat's object from the database
        /// </summary>       
        /// <param name="p_resultStatID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int64 p_resultStatID)
        {
            String sql = "DELETE FROM ResultStat WHERE resultStatID = " + p_resultStatID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an ResultStat's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM ResultStat WHERE " + pWHERE;
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
