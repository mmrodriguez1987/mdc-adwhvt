using System;
using System.Data;
using System.Xml;
using System.Data.SqlClient;
using System.Collections;


namespace DBHelper.SqlHelper
{
       
    
    public class SqlHelper
    {
       
        private static string _connection;

        #region Constructors and Methods
        /// <summary>
        /// Constructor class that initializes the connection       
        /// </summary>
        public SqlHelper(String connectionString)
        {
            _connection = connectionString;
        }

        /// <summary>
        /// Retrieves the connection string through a call to the GetConnectionString method
        /// </summary>
        /// <param name="cnnName">Connection String in String Format</param>
        /// <returns>Connection string in <see cref="System.Data.SqlClient.SqlConnection"/> format</returns>
        /// <exception cref="System.ArgumentNullException">Must specified a valida parameter to generate a valid SqlConnection</exception>
        public static SqlConnection GetConnectString()
        {      
            try
            {
                return new SqlConnection(_connection);
            }
            catch (Exception e)
            {
                throw new Exception("Error on GetConnectString SqlHelper: ", e);
            }
        }


        /// <summary>
        /// Method to attach and arrary of <see cref="System.Data.SqlClient.SqlParameter"/> to a <see cref="System.Data.SqlClient.SqlCommand"/>
        /// The method will assign a DBNull value to any parameter with InpoutOutput direcction also to null values.
        /// <remarks> This procedure will prevent default values from when it is used </remarks>
        /// </summary>
        /// <param name="command">The <see cref="System.Data.SqlClient.SqlCommand"/> which the value will be added the <see cref="System.Data.SqlClient.SqlParameter"/></param>
        /// <param name="commandParameters">Array of <see cref="System.Data.SqlClient.SqlParameter"/> that will be added to the command</param>
        /// <exception cref="System.ArgumentNullException">Must includ a valid command</exception>
        /// <exception cref="System.ArgumentNullException">Must includ a valid arraay</exception>
        private static void AttachParameters(SqlCommand command, SqlParameter[] commandParameters)
        {
            if (command == null)
            {
                throw new ArgumentNullException("command", "Invalid Command");
            }
            if (commandParameters == null)
            {
                throw new ArgumentNullException("commandParameters", "Null Parameters");
            }
            foreach (SqlParameter p in commandParameters)
            {
                if ((p.Direction == ParameterDirection.InputOutput) && (p.Value == null))
                {
                    p.Value = DBNull.Value;
                }
                command.Parameters.Add(p);
            }
        }

        /// <summary>    
        /// This method assign a value's array to a <see cref="System.Data.SqlClient.SqlParameter"/> array        
        /// </summary>
        /// <param name="commandParameters">Array of <see cref="System.Data.SqlClient.SqlParameter"/> that will be assigned the values</param>
        /// <param name="parameterValues">Array of <see cref="System.Object"/>that save the values to assign</param>
        /// <exception cref="System.ArgumentException">Parameter count 'commandParameters' doesn't match with 'parameterValues' quantity</exception>
        private static void AssignParameterValues(SqlParameter[] commandParameters, object[] parameterValues)
        {         

            if ((commandParameters == null) || (parameterValues == null))            
                return;           
            if (commandParameters.Length != (parameterValues.Length))            
                throw new ArgumentException("Parameter count doesn't fit with values count.");           
            for (int i = 0, j = commandParameters.Length; i < j; i++)
            {
                commandParameters[i].Value = parameterValues[i];
            }
        }

        /// <summary>
        /// This method open (if it's needed) and assign a <see cref="System.Data.SqlClient.SqlConnection"/>        
        /// <see cref="System.Data.SqlClient.SqlTransaction"/>, <see cref="System.Data.CommandType"/> and
        /// <see cref="System.Data.SqlClient.SqlParameter"/> to a provided command
        /// </summary>
        /// <param name="command">The <see cref="System.Data.SqlClient.SqlCommand"/> which the value will be added the <see cref="System.Data.SqlClient.SqlParameter"/></param>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">Array of <see cref="System.Data.SqlClient.SqlParameter"/> that will be added to the command</param>
        private static void PrepareCommand(SqlCommand command, SqlConnection connection, SqlTransaction transaction, CommandType commandType, string commandText, SqlParameter[] commandParameters)
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            command.Connection = connection;
            command.CommandText = commandText;
            if (transaction != null)
            {
                command.Transaction = transaction;
            }
            command.CommandType = commandType;
            if (commandParameters != null)
            {
                AttachParameters(command, commandParameters);
            }
            return;
        }
        #endregion  Constructors and Methods

        #region ExecuteNonQuery

        /// <summary>
        /// Execute a SQL Command to the database refered at the connection string.        
        /// </summary>
        /// <example>
        /// Execution sample:        
        /// <code>
        /// int result;
        /// result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// The method doesn't return any dataset.        
        /// </remarks>
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns>A <see cref="System.Int64"/> that represent the affected rows number with the command.</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText)
        {
            return ExecuteNonQuery(connectionString, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SQL Command to the database refered on the connection string, passing a param array
        /// </summary>
        /// <example>
        /// Execution sample: 
        /// <code>
        /// int result;
        /// result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "SpGeneraOrdenesCompra","20120101","20120331");
        /// </code>
        /// </example>
        /// <remarks>
        /// The method doesn't return any dataset.
        /// </remarks>
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of SqlParameters used to execute the command</param>
        /// <returns>A <see cref="System.Int64"/> thta represent the affected rows count with the command</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //create & open a SqlConnection, and dispose of it after we are done.
            using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteNonQuery(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a Stored Procedure using a SqlCommand on the database refered at the connection string with a param arrays
        /// <example>
        /// Exceution sample:
        /// <code>
        /// int result;
        /// result = ExecuteNonQuery(connString, "PublishOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>
        /// The method doesn't return any dataset.
        /// </remarks>
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>A <see cref="System.Int64"/> thta represent the affected rows count with the command</returns>
        public static int ExecuteNonQuery(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>        
        /// Execute a SqlCommand using a provided SqlConnection
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// The method doesn't return any dataset. y no toma ningún parámetro
        /// </remarks>        
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns>A <see cref="System.Int64"/> thta represent the affected rows count with the command</returns>
        public static int ExecuteNonQuery(SqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(connection, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>        
        /// Execute a SqlCommand to the provided SqlConnection on the parammeters
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>
        /// <remarks>
        /// The method doesn't return any dataset. y no toma ningún parámetro 
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of SqlParameters used to execute the command</param>
        /// <returns>A <see cref="System.Int64"/> thta represent the affected rows count with the command</returns>
        public static int ExecuteNonQuery(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SqlCommand cmd = new SqlCommand();
            PrepareCommand(cmd, connection, (SqlTransaction)null, commandType, commandText, commandParameters);

            //finally, execute the command.
            int retval = cmd.ExecuteNonQuery();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a Stored Procedure using a SqlCommand on the database refered at the connection string with a param array values 
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, "PublishOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>
        /// The method doesn't return any dataset. y no toma ningún parámetro
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>A <see cref="System.Int64"/> thta represent the affected rows count with the command</returns>
        public static int ExecuteNonQuery(SqlConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>        
        /// Execute a SqlCommand on a <see cref="System.Data.SqlClient.SqlTransaction"/> provided
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// The method doesn't return any dataset. y no toma ningún parámetro
        /// </remarks>      
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns>A <see cref="System.Int64"/> thta represent the affected rows count with the command</returns>
        public static int ExecuteNonQuery(SqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(transaction, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>        
        /// Execute a SqlCommand on a <see cref="System.Data.SqlClient.SqlTransaction"/> provided
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>
        /// <remarks>
        /// The method doesn't return any dataset. y no toma ningún parámetro
        /// </remarks>         
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of SqlParameters used to execute the command</param>
        /// <returns>A <see cref="System.Int64"/> thta represent the affected rows count with the command</returns>
        public static int ExecuteNonQuery(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SqlCommand cmd = new SqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //finally, execute the command.
            int retval = cmd.ExecuteNonQuery();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a Stored Procedure on a <see cref="System.Data.SqlClient.SqlTransaction"/> provided using a parameter values array object
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, trans, "PublishOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>
        /// The method doesn't return any dataset. y no toma ningún parámetro
        /// </remarks>           
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>A <see cref="System.Int64"/> thta represent the affected rows count with the command</returns>
        public static int ExecuteNonQuery(SqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //si recibimos valores de los parámetros, tenemos que averiguar dónde se dirigen
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //tirar de los parámetros para este procedimiento almacenado desde la memoria caché de parámetros (o descubrirlos y poblar la memoria caché)
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //asignar los valores proporcionados a estos parámetros en base a orden de los parámetros
                AssignParameterValues(commandParameters, parameterValues);

                //llame a la sobrecarga que toma una matriz de SqlParameters asignan los valores proporcionados a estos parámetros según el orden de parámetro
                return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //de lo contrario, sólo puede llamar a la SP sin params
            else
            {
                return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName);
            }
        }


        #endregion ExecuteNonQuery

        #region ExecuteDataSet

        /// <summary>
        /// Execute a SqlCommand on a SqlConnection provided
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// Return the dataset with the required data if there are any error the dataset will 
        /// return a daset.datatable with one only column "error" with the error
        /// </remarks>     
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns>A <see cref="System.Data.DataSet"/> containing the result generated by the Command executed</returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(connectionString, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>       
        /// Execute a SqlCommand on the speficied database on the connection string
        /// </summary>
        /// <example> 
        /// Example:
        /// <code>
        /// DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>
        /// <remarks>
        /// Return the dataset with the required data if there are any error the dataset will.
        /// Return a daset.datatable with one only column "error" with the error
        /// </remarks>     
        /// <param name="connectionString"> valid connection string for <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">The <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure, T-SQL Query</param>
        /// <param name="commandParameters">Param array objects that will be used on the SP or Function</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> Datase with the resultset</returns>      
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {           
            SqlConnection cn; 
            using (cn = new SqlConnection(connectionString))
            {
                cn.Open();                    
                return ExecuteDataset(cn, commandType, commandText, commandParameters);
            }          
        }

        /// <summary>        
        /// Execute a Stored Procedure using a SqlCommand on the database refered at the connection string with a param array values        
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// DataSet ds =  ExecuteDataset(connString, "GetOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>        
        /// Ths method doesn't provide access to OutputParams or the return value of the Stored Procedure
        /// </remarks>  
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>A <see cref="System.Data.DataSet"/> containing the result generated by the Command executed</returns>
        public static DataSet ExecuteDataset(string connectionString, string spName, params object[] parameterValues)
        {
            
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
            
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connectionString, spName);            
                AssignParameterValues(commandParameters, parameterValues);            
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }            
            else            
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName);
            
        }

        /// <summary>        
        /// Execute a SqlCommand on the database refered at the SqlConnection.
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders");
        /// </code>
        /// </example>
        /// <remarks> 
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado. No toma ningun parametro
        /// </remarks>         
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns>A <see cref="System.Data.DataSet"/> containing the result generated by the Command executed</returns>
        public static DataSet ExecuteDataset(SqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(connection, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Ejecuta un SqlCommand sobre una base de datos especificada en un SqlConnection proporcionado    
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>     
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>A <see cref="System.Data.DataSet"/> containing the result generated by the Command executed</returns>
        public static DataSet ExecuteDataset(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //crear un Command y prepararlo para la ejecucion
            SqlCommand cmd = new SqlCommand();
            PrepareCommand(cmd, connection, (SqlTransaction)null, commandType, commandText, commandParameters);
            //crear un DataAdapter y un DataSet
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();
            cmd.CommandTimeout = 400;
            //llenar el DataSet utilizando los valores predeterminados para los nombres de DataTable, etc
            da.Fill(ds);
            //separar los SqlParameters desde el objeto de comando, para que puedan ser utilizados de nuevo.           
            cmd.Parameters.Clear();
            //return el dataset
            connection.Close();
            return ds;
        }

        /// <summary> 
        /// Ejecutar un procedimiento almacenado mediante un SqlCommand contra la base de datos especificada 
        /// en la cadena de conexión utilizando los valores de los parámetros proporcionados. Este método  
        /// consultara la base de datos para descubrir los parámetros para el procedimiento almacenado y asi asignara
        /// los valores basados en el orden de los parámetros. 
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// DataSet ds = ExecuteDataset(conn, "GetOrders", 24, 36)
        /// </code>
        /// </example>
        /// <remarks>
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado
        /// </remarks>         
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>A <see cref="System.Data.DataSet"/> containing the result generated by the Command executed</returns>
        public static DataSet ExecuteDataset(SqlConnection connection, string spName, params object[] parameterValues)
        {
            //si recibimos valores de los parámetros, tenemos que averiguar dónde se dirigen
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //tirar de los parámetros para este procedimiento almacenado desde la memoria caché de parámetros (o descubrirlos y poblar la memoria caché).
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //Asigna los valores proveidos a los Parametros basado en el orden de los parametros
                AssignParameterValues(commandParameters, parameterValues);

                //llama al metodo sobrecargado que toma el arreglo de SqlParameters 
                return ExecuteDataset(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //en otro caso solamente se llama al procedimiento sin parametros
            else
            {
                return ExecuteDataset(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Ejecuta un SqlCommand sobre un <see cref="System.Data.SqlClient.SqlTransaction"/> proveido
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado
        /// </remarks>        
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns>A <see cref="System.Data.DataSet"/> containing the result generated by the Command executed</returns>
        public static DataSet ExecuteDataset(SqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pasar parametros nulo a través de la llamada al metodo sobrecargado para proporcionar el conjunto de SqlParameters
            return ExecuteDataset(transaction, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Ejecuta un SqlCommand sobre un <see cref="System.Data.SqlClient.SqlTransaction"/> proveido
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>
        /// <remarks>
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado
        /// </remarks>        
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>A <see cref="System.Data.DataSet"/> containing the result generated by the Command executed</returns>
        public static DataSet ExecuteDataset(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //crea un ccomando y lo prepara para la ejecucion
            SqlCommand cmd = new SqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);
            //crea el DataAdapter y el DataSet
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();
            //llena el DataSet utilizando los valores predeterminados para los nombres de DataTable, etc
            da.Fill(ds);
            //separa los SqlParameters desde el objeto de comando, para que puedan ser utilizados de nuevo.
            cmd.Parameters.Clear();
            //returna el dataset
            return ds;
        }

        /// <summary>
        /// Ejecutar un procedimiento almacenado mediante un SqlCommand contra la base de datos especificada 
        /// en un <see cref="System.Data.SqlClient.SqlTransaction"/> proveido, utilizando los valores de los parámetros 
        /// proporcionados. Este método  consultara la base de datos para descubrir los parámetros para el procedimiento 
        /// almacenado y asi asignara los valores basados en el orden de los parámetros.    
        /// </summary>
        /// <example>
        /// Exceution sample:
        /// <code>
        /// DataSet ds = ExecuteDataset(trans, "GetOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado
        /// </remarks>   
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>A <see cref="System.Data.DataSet"/> containing the result generated by the Command executed</returns>
        public static DataSet ExecuteDataset(SqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //si recibimos valores de los parámetros, tenemos que averiguar dónde se dirigen
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //tirar de los parámetros para este procedimiento almacenado desde la memoria caché de parámetros (o descubrirlos y poblar la memoria caché).
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //Asigna los valores proveidos a los Parametros basado en el orden de los parametros
                AssignParameterValues(commandParameters, parameterValues);

                //llama al metodo sobrecargado que toma el arreglo de SqlParameters 
                return ExecuteDataset(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //en otro caso solamente se llama al procedimiento sin parametros
            else
            {
                return ExecuteDataset(transaction, CommandType.StoredProcedure, spName);
            }
        }
        

        #endregion ExecuteDataSet

        #region ExecuteReader

        /// <summary>
        /// Esta enumeración se utiliza para indicar si la conexión fue proporcionada por la persona que llama, 
        /// o se creo por la clase SkyHelper, de modo que podemos establecer el CommandBehavior adecuado cuando 
        /// se llama a ExecuteReader() pasan a través de la llamada prestación nulo para el conjunto de SqlParameters
        /// </summary>
        private enum SqlConnectionOwnership
        {
            /// <summary>Conexion pertenece y es administrada por SkyHelper</summary>
            Internal,
            /// <summary>Conexion pertenece y es administrada por quien hace el llamado al metodo</summary>
            External
        }

        /// <summary>
        /// Crea y prepara un 'SqlCommand', y hace una llamada al 'ExecuteReader' con el 'CommandBehavior' apropiado       
        /// </summary>
        /// <remarks>
        /// Si se crea y se abre la conexión, y queremos que la conexión se cierre cuando el DataReader es closed.Connection es propiedad y está gestionado por la persona que llama
        /// Si quien hace el llamado al metodo es quien proporciona la conexion, entonces la administracion de la misma queda en mano de quien hace el llamado
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">an array of SqlParameters to be associated with the command or 'null' if no parameters are required</param>
        /// <param name="connectionOwnership">indicates whether the connection parameter was provided by the caller, or created by SqlHelper</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        private static SqlDataReader ExecuteReader(SqlConnection connection, SqlTransaction transaction, CommandType commandType, string commandText, SqlParameter[] commandParameters, SqlConnectionOwnership connectionOwnership)
        {

            //create a command and prepare it for execution
            SqlCommand cmd = new SqlCommand();
            PrepareCommand(cmd, connection, transaction, commandType, commandText, commandParameters);

            //create a reader
            SqlDataReader dr;

            // call ExecuteReader with the appropriate CommandBehavior
            if (connectionOwnership == SqlConnectionOwnership.External)
            {
                dr = cmd.ExecuteReader();
            }
            else
            {
                dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();

            return dr;
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        public static SqlDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteReader(connectionString, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        public static SqlDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //create & open a SqlConnection
            SqlConnection cn = new SqlConnection(connectionString);
            cn.Open();

            try
            {
                //call the private overload that takes an internally owned connection in place of the connection string
                return ExecuteReader(cn, null, commandType, commandText, commandParameters, SqlConnectionOwnership.Internal);
            }
            catch
            {
                //if we fail to return the SqlDatReader, we need to close the connection ourselves
                cn.Close();
                throw;
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        public static SqlDataReader ExecuteReader(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteReader(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteReader(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        public static SqlDataReader ExecuteReader(SqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteReader(connection, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        public static SqlDataReader ExecuteReader(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //pass through the call to the private overload using a null transaction value and an externally owned connection
            return ExecuteReader(connection, (SqlTransaction)null, commandType, commandText, commandParameters, SqlConnectionOwnership.External);
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        public static SqlDataReader ExecuteReader(SqlConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                AssignParameterValues(commandParameters, parameterValues);

                return ExecuteReader(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteReader(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        public static SqlDataReader ExecuteReader(SqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteReader(transaction, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///   SqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        public static SqlDataReader ExecuteReader(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //pass through to private overload, indicating that the connection is owned by the caller
            return ExecuteReader(transaction.Connection, transaction, commandType, commandText, commandParameters, SqlConnectionOwnership.External);
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified
        /// SqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(trans, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> containing the results of the command executed</returns>
        public static SqlDataReader ExecuteReader(SqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                AssignParameterValues(commandParameters, parameterValues);

                return ExecuteReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteReader(transaction, CommandType.StoredProcedure, spName);
            }
        }

       

        #endregion ExecuteReader

        #region ExecuteScalar

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(connectionString, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //create & open a SqlConnection, and dispose of it after we are done.
            using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteScalar(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, "GetOrderCount", 24, 36);
        /// </remarks>
        /// <param name="connectionString">A valid connection string for the <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(connection, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SqlCommand cmd = new SqlCommand();
            PrepareCommand(cmd, connection, (SqlTransaction)null, commandType, commandText, commandParameters);

            //execute the command & return the results
            object retval = cmd.ExecuteScalar();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;

        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, "GetOrderCount", 24, 36);
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SqlConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteScalar(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteScalar(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">The Stored Procedure name, T-SQL Query, etc</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(transaction, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">The Stored Procedure name, T-SQL Query, etc</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SqlCommand cmd = new SqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //execute the command & return the results
            object retval = cmd.ExecuteScalar();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the specified
        /// SqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, "GetOrderCount", 24, 36);
        /// </remarks>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteScalar(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteScalar(transaction, CommandType.StoredProcedure, spName);
            }
        }

        #endregion ExecuteScalar

        #region ExecuteXmlReader

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">The Stored Procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteXmlReader(connection, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">The Stored Procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SqlCommand cmd = new SqlCommand();
            PrepareCommand(cmd, connection, (SqlTransaction)null, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            XmlReader retval = cmd.ExecuteXmlReader();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;

        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="connection">A <see cref="System.Data.SqlClient.SqlConnection"/> that will be executed on the command</param>
        /// <param name="spName">The name of the stored procedure using "FOR XML AUTO"</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SqlConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">The Stored Procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteXmlReader(transaction, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="commandType">A <see cref="System.Data.CommandType"/> (Stored Procedure, query, etc)</param>
        /// <param name="commandText">The Stored Procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <param name="commandParameters">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SqlCommand cmd = new SqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            XmlReader retval = cmd.ExecuteXmlReader();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified 
        /// SqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="transaction">A valid <see cref="System.Data.SqlClient.SqlTransaction"/> or 'null'</param>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameterValues">An array of Object parameters that will be assigned as input parameters to the SP</param>
        /// <returns>A <see cref="System.Data.DataSet"/> containing the result generated by the Command executed</returns>
        public static XmlReader ExecuteXmlReader(SqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName);
            }
        }

        #endregion ExecuteXmlReader

        #region UpdateDataset
        /// <summary>
        /// Executes the respective command for each inserted, updated, or deleted row in the DataSet.
        /// </summary>
        /// <param name="insertCommand">A valid transact-SQL statement or stored procedure to insert new records into the data source</param>
        /// <param name="updateCommand">A valid transact-SQL statement or stored procedure used to update records in the data source</param>
        /// <param name="deleteCommand">A valid transact-SQL statement or stored procedure to delete records from the data source</param>
        /// <param name="dataSet">the DataSet used to update the data source</param>
        /// <param name="tableName">the DataTable used to update the data source</param>
        public static void UpdateDataset(SqlCommand insertCommand, SqlCommand updateCommand, SqlCommand deleteCommand, DataSet dataSet, String tableName)
        {
            if (insertCommand == null) throw new ArgumentNullException("insertcommand");
            if (updateCommand == null) throw new ArgumentNullException("updateCommand");
            if (deleteCommand == null) throw new ArgumentNullException("deleteCommand");
            if (dataSet == null) throw new ArgumentNullException("dataSet");
            if (tableName == null || tableName.Length == 0) throw new ArgumentNullException("tableName");

            SqlDataAdapter dataAdapter = new SqlDataAdapter();
            try
            {
                dataAdapter.UpdateCommand = insertCommand;
                dataAdapter.InsertCommand = updateCommand;
                dataAdapter.DeleteCommand = deleteCommand;

                dataAdapter.Update(dataSet, tableName);
                dataSet.AcceptChanges();

            }
            finally { if (dataAdapter != null) dataAdapter.Dispose(); }

        }

        /// <summary>
        /// Executes the respective command for each inserted, updated, or deleted row in the DataSet.
        /// </summary>
        /// <param name="insertCommand">A valid transact-SQL statement or stored procedure to insert new records into the data source</param>
        /// <param name="updateCommand">A valid transact-SQL statement or stored procedure used to update records in the data source</param>
        /// <param name="dataSet">the DataSet used to update the data source</param>
        /// <param name="tableName">the DataTable used to update the data source</param>
        public static void UpdateDataset(SqlCommand insertCommand, SqlCommand updateCommand, DataSet dataSet, String tableName)
        {
            if (insertCommand == null) throw new ArgumentNullException("insertcommand");
            if (updateCommand == null) throw new ArgumentNullException("updateCommand");
            //if (deleteCommand == null) throw new ArgumentNullException("deleteCommand");
            if (dataSet == null) throw new ArgumentNullException("dataSet");
            if (tableName == null || tableName.Length == 0) throw new ArgumentNullException("tableName");

            SqlDataAdapter dataAdapter = new SqlDataAdapter();
            try
            {
                dataAdapter.UpdateCommand = insertCommand;
                dataAdapter.InsertCommand = updateCommand;
                //dataAdapter.DeleteCommand = deleteCommand;

                dataAdapter.Update(dataSet, tableName);
                dataSet.AcceptChanges();

            }
            finally { if (dataAdapter != null) dataAdapter.Dispose(); }

        }
        #endregion 
    }
}