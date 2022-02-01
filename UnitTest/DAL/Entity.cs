
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
    /// <class>Entity</class>
    /// </summary>
    public class Entity
    {
        #region system variables
        protected string _error;
        protected string _ccn;
        #endregion
        
        #region database variables
	    protected Int64 p_entityID; 
	    protected Int16 p_sourceID; 
	    protected String p_entityQualifyName; 
	    protected String p_entityShortName; 
	    protected String p_typeEntity; 
	    protected Boolean p_active; 
        #endregion

        #region attributes
        
        public Int64 EntityID
        {
            get => p_entityID;            
            set => p_entityID = value;               
        }
        public Int16 SourceID
        {
            get => p_sourceID;            
            set => p_sourceID = value;               
        }
        public String EntityQualifyName
        {
            get => p_entityQualifyName;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 50) {
                        throw new ArgumentOutOfRangeException("entityQualifyName", value.ToString(),
                            "Invalid value for entityQualifyName. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (50).");
                    }
                }
                p_entityQualifyName = value;
            }                           
        }
        public String EntityShortName
        {
            get => p_entityShortName;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 50) {
                        throw new ArgumentOutOfRangeException("entityShortName", value.ToString(),
                            "Invalid value for entityShortName. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (50).");
                    }
                }
                p_entityShortName = value;
            }                           
        }
        public String TypeEntity
        {
            get => p_typeEntity;            
            set {
                if (!String.IsNullOrEmpty(value.ToString())) {
                    if (value.Length > 1) {
                        throw new ArgumentOutOfRangeException("typeEntity", value.ToString(),
                            "Invalid value for typeEntity. Description: String lenght ("
                            + value.Length + ") exceeds maximum value of (1).");
                    }
                }
                p_typeEntity = value;
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
        public Entity(string ccn)
        {  
            _ccn = ccn;
            _error = String.Empty;            
          
        }
        #endregion        
               
        #region GetObject Methods
        /// <summary>
        /// Get and object Entity from the database 
        /// <param name="p_entityID">Entity primary key</param>      
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist</returns>        
        public Boolean GetObject(Int64 p_entityID)
        {
            String sql = "SELECT * FROM Entity WHERE entityID = " + Convert.ToString(p_entityID);       
            
            SqlDataReader dr = null;
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                
                if (dr.Read()) {
				p_entityID = Convert.ToInt64(dr["entityID"]);
				p_sourceID = (dr["sourceID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["sourceID"]));				
				p_entityQualifyName = (dr["entityQualifyName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["entityQualifyName"]));				
				p_entityShortName = (dr["entityShortName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["entityShortName"]));				
				p_typeEntity = (dr["typeEntity"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["typeEntity"]));				
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
        /// Get an Entity object using a filter to delimite the result.
        /// </summary>          
        /// <param name="pFilter">Filter to delimite the consult</param>
        /// <returns>'True' if the object was founded and created, 'False' if the object doesn't exist </returns>      
        public Boolean GetObjectByFilter(string pFilter)
        {
            SqlDataReader dr=null;
            String sql = "SELECT * FROM Entity WHERE " + pFilter;          
            try {
                dr = SqlHelper.ExecuteReader(_ccn, CommandType.Text, sql);
                if (dr.Read()) {
				    p_entityID = Convert.ToInt64(dr["entityID"]);
				    p_sourceID = (dr["sourceID"] == System.DBNull.Value) ? (Convert.ToInt16(null)) : (Convert.ToInt16(dr["sourceID"]));				
				    p_entityQualifyName = (dr["entityQualifyName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["entityQualifyName"]));				
				    p_entityShortName = (dr["entityShortName"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["entityShortName"]));				
				    p_typeEntity = (dr["typeEntity"] == System.DBNull.Value) ? (Convert.ToString(null)) : (Convert.ToString(dr["typeEntity"]));				
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
        /// Get a set of data of 'Entity' represented into a DataTable                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataTable dataT;            
        ///     ...
        ///     dataT = Entity.GetObjectDT(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataTable with a set of 'Entity' objects</returns>
        public DataTable GetObjectDT(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM Entity ";
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;

            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "Entity";
                return ds.Tables[0];
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        
        /// <summary>
        /// Get a set of data of 'Entity' represented into a DataSet                 
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     DataSet dataS;            
        ///     ...
        ///     dataS = Entity.GetObjectDS(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'Entity' objects</returns>
        public DataSet GetObjectDS(String sFilter, String OrderBy, String pFields)
        {
            DataSet ds;
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM Entity ";            
            
            sql += (!String.IsNullOrEmpty(sFilter)) ? (" WHERE " + sFilter) : String.Empty;           
            sql += (!String.IsNullOrEmpty(OrderBy)) ? (" ORDER BY " + OrderBy) : String.Empty;
            
            try {
                ds = SqlHelper.ExecuteDataset(_ccn, CommandType.Text, sql);
                ds.Tables[0].TableName = "Entity";
                return ds;
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
        
        
        /// <summary>
        /// Get a set of data of 'Entity' represented into a SqlDataReader        
        /// </summary>
        /// <code>
        ///     ...
        ///     String Filtro = "Campo1 = 'DS-256' and Fecha = '25/05/2014'";
        ///     String orden = "Fecha,Estado"
        ///     ...
        ///     SqlDataReader dr;            
        ///     ...
        ///     dr = Entity.GetObjectDR(Filtro,Orden,"*");
        /// </code>
        /// <param name="sFilter">Condition to filter the returned data(WHERE)</param>
        /// <param name="OrderBy">Columns Short Order</param>
        /// <param name="pFields">Columns to include in the query</param>
        /// <remarks> Filter and conditions use T-SQL language</remarks>
        /// <returns> Return a DataSet with a set of 'Entity' objects</returns>
        public SqlDataReader GetObjectDR(String sFilter, String OrderBy, String pFields)
        {
            String sql = "SELECT " + ((pFields == String.Empty) ? "*" : pFields) + " FROM Entity ";
           
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
        /// Insert a new data instance into 'Entity' table, make a new record 
        /// </summary>
        public void Insert()
        {
            String sCommand = "INSERT INTO Entity("
		        + "sourceID,entityQualifyName,entityShortName,typeEntity,active) VALUES ("	       
		        + "@sourceID,@entityQualifyName,@entityShortName,@typeEntity,@active)"
		        + " SELECT "
		        + "@entityID = entityID FROM Entity WHERE "
		        + "entityID = SCOPE_IDENTITY()";
           
            SqlParameter[] sqlparams = new SqlParameter[6];            
            sqlparams[0] = new SqlParameter("@entityID", SqlDbType.BigInt);	
            sqlparams[0].Direction = ParameterDirection.Output;
            sqlparams[1] = new SqlParameter("@sourceID", SqlDbType.SmallInt);	
            sqlparams[1].Value = DBNull.Value.Equals(p_sourceID) ? Convert.ToString(DBNull.Value) : p_sourceID;
            sqlparams[2] = new SqlParameter("@entityQualifyName", SqlDbType.VarChar);	
            sqlparams[2].Value = DBNull.Value.Equals(p_entityQualifyName) ? Convert.ToString(DBNull.Value) : p_entityQualifyName;
            sqlparams[3] = new SqlParameter("@entityShortName", SqlDbType.VarChar);	
            sqlparams[3].Value = DBNull.Value.Equals(p_entityShortName) ? Convert.ToString(DBNull.Value) : p_entityShortName;
            sqlparams[4] = new SqlParameter("@typeEntity", SqlDbType.VarChar);	
            sqlparams[4].Value = DBNull.Value.Equals(p_typeEntity) ? Convert.ToString(DBNull.Value) : p_typeEntity;
            sqlparams[5] = new SqlParameter("@active", SqlDbType.Bit);	
            sqlparams[5].Value = DBNull.Value.Equals(p_active) ? Convert.ToString(DBNull.Value) : p_active;
    
            try {
                SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sCommand, sqlparams);
			    p_entityID = Convert.ToInt64(sqlparams[0].Value); 
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }        
        
        #endregion 
        
        #region Metodos Update
        
        /// <summary>
        /// Make an upded to 'Entity' using the object in the class over the database
        /// </summary>
        public void Update()
        { 
            String sCommand = "UPDATE Entity SET "
		        + "sourceID=@sourceID, entityQualifyName=@entityQualifyName, entityShortName=@entityShortName, typeEntity=@typeEntity, active=@active WHERE "		  
		        + "entityID = @entityID";		
	
            SqlParameter[] sqlparams = new SqlParameter[6];
            sqlparams[0] = new SqlParameter("@entityID",SqlDbType.BigInt);
            sqlparams[0].Value = DBNull.Value.Equals(p_entityID) ? Convert.ToInt64(DBNull.Value) : p_entityID;
            sqlparams[1] = new SqlParameter("@sourceID",SqlDbType.SmallInt);
            sqlparams[1].Value = DBNull.Value.Equals(p_sourceID) ? Convert.ToInt16(DBNull.Value) : p_sourceID;
            sqlparams[2] = new SqlParameter("@entityQualifyName",SqlDbType.VarChar);
            sqlparams[2].Value = DBNull.Value.Equals(p_entityQualifyName) ? Convert.ToString(DBNull.Value) : p_entityQualifyName;
            sqlparams[3] = new SqlParameter("@entityShortName",SqlDbType.VarChar);
            sqlparams[3].Value = DBNull.Value.Equals(p_entityShortName) ? Convert.ToString(DBNull.Value) : p_entityShortName;
            sqlparams[4] = new SqlParameter("@typeEntity",SqlDbType.VarChar);
            sqlparams[4].Value = DBNull.Value.Equals(p_typeEntity) ? Convert.ToString(DBNull.Value) : p_typeEntity;
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
        ///  Delete an Entity's object from the database
        /// </summary>
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete()
        {
            String sql = "DELETE FROM Entity WHERE entityID = " + p_entityID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
      
        /// <summary>
        ///  Delete an Entity's object from the database
        /// </summary>       
        /// <param name="p_entityID">Primary key as parameter to delete</param>      
        /// <returns>Return an integer with the affected rows count.</returns>
        public Int64 Delete(Int64 p_entityID)
        {
            String sql = "DELETE FROM Entity WHERE entityID = " + p_entityID;
            try {
                
                return SqlHelper.ExecuteNonQuery(_ccn, CommandType.Text, sql);
            }
            catch (Exception e) {
                _error = e.ToString();
                throw new Exception(_error);
            }
        }
       
        /// <summary>
        ///  Delete an Entity's object from the database using a where to delimit
        /// </summary>
        /// <param name="pWHERE">conditional to apply</param>
        // <returns>Return  an integer that indicate the affected rows count.</returns>
        public Int64 DeleteByFilter(String pWHERE)
        {
            String sql = "DELETE FROM Entity WHERE " + pWHERE;
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
