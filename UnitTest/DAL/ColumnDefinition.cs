
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
    /// <class>ColumnDefinition</class>
    /// </summary>
    public class ColumnDefinition
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int64 p_columnID; 
	    protected Int64 p_entityID; 
	    protected String p_columnName; 
	    protected String p_description; 
	    protected Int32 p_ordinalPosition; 
	    protected Boolean p_active; 
        #endregion

        #region attributes
        
        public Int64 ColumnID
        {
            get => p_columnID;            
            set => p_columnID = value;               
        }
        public Int64 EntityID
        {
            get => p_entityID;            
            set => p_entityID = value;               
        }
        public String ColumnName
        {
            get => p_columnName;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 50) {
                        throw new ArgumentOutOfRangeException("columnName", value.ToString(),
                            "Invalid value for columnName. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (50).");
                    }
                }
                p_columnName = value;
            }                           
        }
        public String Description
        {
            get => p_description;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 300) {
                        throw new ArgumentOutOfRangeException("description", value.ToString(),
                            "Invalid value for description. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (300).");
                    }
                }
                p_description = value;
            }                           
        }
        public Int32 OrdinalPosition
        {
            get => p_ordinalPosition;            
            set => p_ordinalPosition = value;               
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
        public ColumnDefinition(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object ColumnDefinition from the database 
        /// <param name="p_columnID">ColumnDefinition primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int64 p_columnID)
        {
            String sql = "SELECT * FROM ColumnDefinition WHERE columnID = " + Convert.ToString(p_columnID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_columnID = Convert.ToInt64(dr["columnID"]);
				p_entityID = (dr["entityID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["entityID"]));				
				p_columnName = (dr["columnName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["columnName"]));				
				p_description = (dr["description"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["description"]));				
				p_ordinalPosition = (dr["ordinalPosition"] == System.DBNull.Value) ? (Convert.ToInt32(null)) : (Convert.ToInt32(dr["ordinalPosition"]));				
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
        /// Get an ColumnDefinition object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM ColumnDefinition WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_columnID = Convert.ToInt64(dr["columnID"]);
				    p_entityID = (dr["entityID"] == System.DBNull.Value) ? (Convert.ToInt64(null)) : (Convert.ToInt64(dr["entityID"]));				
				    p_columnName = (dr["columnName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["columnName"]));				
				    p_description = (dr["description"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["description"]));				
				    p_ordinalPosition = (dr["ordinalPosition"] == System.DBNull.Value) ? (Convert.ToInt32(null)) : (Convert.ToInt32(dr["ordinalPosition"]));				
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
        /// Get a set of data of 'ColumnDefinition' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = ColumnDefinition.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'ColumnDefinition' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ColumnDefinition ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "ColumnDefinition";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'ColumnDefinition' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = ColumnDefinition.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'ColumnDefinition' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ColumnDefinition ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "ColumnDefinition";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'ColumnDefinition' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = ColumnDefinition.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'ColumnDefinition' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM ColumnDefinition ";
           
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
        /// Insert a new data instance into 'ColumnDefinition' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO ColumnDefinition("
		        + "entityID,columnName,description,ordinalPosition,active) VALUES ("	       
		        + "@entityID,@columnName,@description,@ordinalPosition,@active)"
		        + " SELECT "
		        + "@columnID = columnID FROM ColumnDefinition WHERE "
		        + "columnID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[6];            
            sqlparams[0] = new SqlParameter("@columnID", SqlDbType.BigInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@entityID", SqlDbType.BigInt);	
            sqlparams[1].Value = DBNull.Value.Equals(p_entityID) ? Convert.ToString(DBNull.Value) : p_entityID;
            sqlparams[2] = new SqlParameter("@columnName", SqlDbType.VarChar);	
            sqlparams[2].Value = DBNull.Value.Equals(p_columnName) ? Convert.ToString(DBNull.Value) : p_columnName;
            sqlparams[3] = new SqlParameter("@description", SqlDbType.VarChar);	
            sqlparams[3].Value = DBNull.Value.Equals(p_description) ? Convert.ToString(DBNull.Value) : p_description;
            sqlparams[4] = new SqlParameter("@ordinalPosition", SqlDbType.Int);	
            sqlparams[4].Value = DBNull.Value.Equals(p_ordinalPosition) ? Convert.ToString(DBNull.Value) : p_ordinalPosition;
            sqlparams[5] = new SqlParameter("@active", SqlDbType.Bit);	
            sqlparams[5].Value = DBNull.Value.Equals(p_active) ? Convert.ToString(DBNull.Value) : p_active;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_columnID = Convert.ToInt64(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'ColumnDefinition' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE ColumnDefinition SET "
		        + "entityID=@entityID, columnName=@columnName, description=@description, ordinalPosition=@ordinalPosition, active=@active WHERE "		  
		        + "columnID = @columnID";		
	
            SqlParameter[] sqlparams = new SqlParameter[6];
            sqlparams[0] = new SqlParameter("@columnID",SqlDbType.BigInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_columnID) ? Convert.ToInt64(DBNull.Value) : p_columnID;
            sqlparams[1] = new SqlParameter("@entityID",SqlDbType.BigInt);
            sqlparams[1].Value = DBNull.Value.Equals(p_entityID) ? Convert.ToInt64(DBNull.Value) : p_entityID;
            sqlparams[2] = new SqlParameter("@columnName",SqlDbType.VarChar);
            sqlparams[2].Value = DBNull.Value.Equals(p_columnName) ? Convert.ToString(DBNull.Value) : p_columnName;
            sqlparams[3] = new SqlParameter("@description",SqlDbType.VarChar);
            sqlparams[3].Value = DBNull.Value.Equals(p_description) ? Convert.ToString(DBNull.Value) : p_description;
            sqlparams[4] = new SqlParameter("@ordinalPosition",SqlDbType.Int);
            sqlparams[4].Value = DBNull.Value.Equals(p_ordinalPosition) ? Convert.ToInt32(DBNull.Value) : p_ordinalPosition;
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
        ///  Delete an ColumnDefinition's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM ColumnDefinition WHERE columnID = " + p_columnID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an ColumnDefinition's object from the database
        /// </summary>       
        /// <param name="p_columnID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int64 p_columnID)
        {
            String sql = "DELETE FROM ColumnDefinition WHERE columnID = " + p_columnID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an ColumnDefinition's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM ColumnDefinition WHERE " + pWHERE;
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
