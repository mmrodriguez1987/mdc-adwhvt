
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
    /// <class>State</class>
    /// </summary>
    public class State
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int16 p_stateID; 
	    protected String p_stateName; 
	    protected String p_stateMessage; 
	    protected Boolean p_active; 
        #endregion

        #region attributes
        
        public Int16 StateID
        {
            get => p_stateID;            
            set => p_stateID = value;               
        }
        public String StateName
        {
            get => p_stateName;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 50) {
                        throw new ArgumentOutOfRangeException("stateName", value.ToString(),
                            "Invalid value for stateName. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (50).");
                    }
                }
                p_stateName = value;
            }                           
        }
        public String StateMessage
        {
            get => p_stateMessage;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 300) {
                        throw new ArgumentOutOfRangeException("stateMessage", value.ToString(),
                            "Invalid value for stateMessage. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (300).");
                    }
                }
                p_stateMessage = value;
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
        public State(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object State from the database 
        /// <param name="p_stateID">State primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int16 p_stateID)
        {
            String sql = "SELECT * FROM State WHERE stateID = " + Convert.ToString(p_stateID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_stateID = Convert.ToInt16(dr["stateID"]);
				p_stateName = (dr["stateName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["stateName"]));				
				p_stateMessage = (dr["stateMessage"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["stateMessage"]));				
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
        /// Get an State object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM State WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_stateID = Convert.ToInt16(dr["stateID"]);
				    p_stateName = (dr["stateName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["stateName"]));				
				    p_stateMessage = (dr["stateMessage"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["stateMessage"]));				
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
        /// Get a set of data of 'State' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = State.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'State' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM State ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "State";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'State' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = State.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'State' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM State ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "State";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'State' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = State.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'State' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM State ";
           
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
        /// Insert a new data instance into 'State' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO State("
		        + "stateName,stateMessage,active) VALUES ("	       
		        + "@stateName,@stateMessage,@active)"
		        + " SELECT "
		        + "@stateID = stateID FROM State WHERE "
		        + "stateID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[4];            
            sqlparams[0] = new SqlParameter("@stateID", SqlDbType.SmallInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@stateName", SqlDbType.VarChar);	
            sqlparams[1].Value = DBNull.Value.Equals(p_stateName) ? Convert.ToString(DBNull.Value) : p_stateName;
            sqlparams[2] = new SqlParameter("@stateMessage", SqlDbType.VarChar);	
            sqlparams[2].Value = DBNull.Value.Equals(p_stateMessage) ? Convert.ToString(DBNull.Value) : p_stateMessage;
            sqlparams[3] = new SqlParameter("@active", SqlDbType.Bit);	
            sqlparams[3].Value = DBNull.Value.Equals(p_active) ? Convert.ToString(DBNull.Value) : p_active;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_stateID = Convert.ToInt16(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'State' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE State SET "
		        + "stateName=@stateName, stateMessage=@stateMessage, active=@active WHERE "		  
		        + "stateID = @stateID";		
	
            SqlParameter[] sqlparams = new SqlParameter[4];
            sqlparams[0] = new SqlParameter("@stateID",SqlDbType.SmallInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_stateID) ? Convert.ToInt16(DBNull.Value) : p_stateID;
            sqlparams[1] = new SqlParameter("@stateName",SqlDbType.VarChar);
            sqlparams[1].Value = DBNull.Value.Equals(p_stateName) ? Convert.ToString(DBNull.Value) : p_stateName;
            sqlparams[2] = new SqlParameter("@stateMessage",SqlDbType.VarChar);
            sqlparams[2].Value = DBNull.Value.Equals(p_stateMessage) ? Convert.ToString(DBNull.Value) : p_stateMessage;
            sqlparams[3] = new SqlParameter("@active",SqlDbType.Bit);
            sqlparams[3].Value = DBNull.Value.Equals(p_active) ? Convert.ToBoolean(DBNull.Value) : p_active;
    
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
        ///  Delete an State's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM State WHERE stateID = " + p_stateID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an State's object from the database
        /// </summary>       
        /// <param name="p_stateID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int16 p_stateID)
        {
            String sql = "DELETE FROM State WHERE stateID = " + p_stateID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an State's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM State WHERE " + pWHERE;
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
