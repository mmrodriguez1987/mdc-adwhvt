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

        #region Metodos Construnctores y utilitarios
        /// <summary>
        /// Constructor unico de la clase que inicializa las Conexiónes en la clase SkyConnection
        /// </summary>
        public SqlHelper(String connectionString)
        {
            _connection = connectionString;
        }

        /// <summary>
        /// Recupera la cadena de conexión a traves de un llamado al metodo GiveConnection
        /// </summary>
        /// <param name="cnnName">Cadena de Coenxion elegida</param>
        /// <returns>Cadena de Conexión en formato <see cref="System.Data.SqlClient.SqlConnection"/></returns>
        /// <exception cref="System.ArgumentNullException">Se debe especificar un parametro valido para generar una cadena de conexion consistente</exception>
        public static SqlConnection GetConnectString()
        {      
            try
            {
                return new SqlConnection(_connection);
            }
            catch (Exception e)
            {
                throw new Exception("Error en el Método GetConnectString en clase SqlHelper ", e);
            }
        }


        /// <summary>
        /// Este metodo es usado para adjuntar un arreglo de <see cref="System.Data.SqlClient.SqlParameter"/> a 
        /// un <see cref="System.Data.SqlClient.SqlCommand"/> cabe destacar que el metodo asignara un valor de 
        /// DbNull a cualquier parametro con una direccion de InputOutput y a valores Nulos. 
        /// <remarks>Este procedimiento prevendra valores por defecto desde que se hace uso</remarks>
        /// </summary>
        /// <param name="command">El <see cref="System.Data.SqlClient.SqlCommand"/> al cual seran agregados los <see cref="System.Data.SqlClient.SqlParameter"/></param>
        /// <param name="commandParameters">Un arreglo de <see cref="System.Data.SqlClient.SqlParameter"/> que sera agregado al command</param>
        /// <exception cref="System.ArgumentNullException">Se debe incluir un command valido</exception>
        /// <exception cref="System.ArgumentNullException">Se debe incluir un arreglo valido</exception>
        private static void AttachParameters(SqlCommand command, SqlParameter[] commandParameters)
        {
            if (command == null)
            {
                throw new ArgumentNullException("command", "Comando invalido");
            }
            if (commandParameters == null)
            {
                throw new ArgumentNullException("commandParameters", "Parametros Nulos");
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
        /// Este metodo asigna un arreglo de valores a un arreglo de <see cref="System.Data.SqlClient.SqlParameter"/>.
        /// </summary>
        /// <param name="commandParameters">Arreglo de <see cref="System.Data.SqlClient.SqlParameter"/> al que seran asignados los valores</param>
        /// <param name="parameterValues">Arreglo de <see cref="System.Object"/> que guarda los valores a asignar</param>
        /// <exception cref="System.ArgumentException">Conteo de parametros 'commandParameters' no concuerda con cantidad de valores en 'parameterValues'</exception>
        private static void AssignParameterValues(SqlParameter[] commandParameters, object[] parameterValues)
        {
            if ((commandParameters == null) || (parameterValues == null))
            {
                return;
            }
            if (commandParameters.Length != parameterValues.Length)
            {
                throw new ArgumentException("Conteo de Parametros no concuerda con cantidad de valores.");
            }
            for (int i = 0, j = commandParameters.Length; i < j; i++)
            {
                commandParameters[i].Value = parameterValues[i];
            }
        }

        /// <summary>
        /// Este metodo abre (Si es necesario) y asigna un <see cref="System.Data.SqlClient.SqlConnection"/>, 
        /// <see cref="System.Data.SqlClient.SqlTransaction"/>, <see cref="System.Data.CommandType"/> y 
        /// <see cref="System.Data.SqlClient.SqlParameter"/> 
        /// a un comando proveido
        /// </summary>
        /// <param name="command">El <see cref="System.Data.SqlClient.SqlCommand"/> que sera preparado</param>
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="transaction">Un <see cref="System.Data.SqlClient.SqlTransaction"/> valido o un valor 'null'</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un Arreglo de <see cref="System.Data.SqlClient.SqlParameter"/> para ser asociado con el comando o 'null' si el parametro no es requerido</param>
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
        #endregion Metodos Construnctores y utilitarios

        #region ExecuteNonQuery

        /// <summary>
        /// Ejecutar un SqlCommand contra la base de datos especificada en la cadena de conexión.
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// int result;
        /// result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// No devuelve ningún conjunto de resultados y no toma ningún parámetro
        /// </remarks>
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <returns>Un <see cref="System.Int64"/> que representa el numero de filas afectadas con el comando</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText)
        {
            return ExecuteNonQuery(connectionString, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Ejecutar un SqlCommand contra la base de datos especificada en la cadena de conexión, 
        /// haciendo uso de arreglos de parametros.       
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// int result;
        /// result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "SpGeneraOrdenesCompra","20120101","20120331");
        /// </code>
        /// </example>
        /// <remarks>
        /// No devuelve ningún conjunto de resultados
        /// </remarks>
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de SqlParameters usados para ejecutar el comando</param>
        /// <returns>Un <see cref="System.Int64"/> que representa el numero de filas afectadas con el comando</returns>
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
        /// Ejecutar un procedimiento almacenado mediante un SqlCommand contra la base de datos especificada 
        /// en la cadena de conexión utilizando los valores de los parámetros proporcionados. Este método  
        /// consultara la base de datos para descubrir los parámetros para el procedimiento almacenado y asi asignara
        /// los valores basados en el orden de los parámetros.  
        /// </summary>         
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// int result;
        /// result = ExecuteNonQuery(connString, "PublishOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>
        /// No devuelve ningún conjunto de resultados y no toma ningún parámetro
        /// </remarks>
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Int64"/> que representa el numero de filas afectadas con el comando</returns>
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
        /// Ejecuta un SqlCommand contra un SqlConnection proporcionado       
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// No devuelve ningún conjunto de resultados y no toma ningún parámetro
        /// </remarks>        
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <returns>Un <see cref="System.Int64"/> que representa el numero de filas afectadas con el comando</returns>
        public static int ExecuteNonQuery(SqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(connection, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Ejecuta un SqlCommand contra el SqlConnection proporcionado utilizando los parámetros proporcionados.
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>
        /// <remarks>
        /// No devuelve ningún conjunto de resultados y no toma ningún parámetro 
        /// </remarks>
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de SqlParameters usados para ejecutar el comando</param>
        /// <returns>Un <see cref="System.Int64"/> que representa el numero de filas afectadas con el comando</returns>
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
        /// Ejecutar un procedimiento almacenado mediante un SqlCommand contra la base de datos especificada 
        /// en la cadena de conexión utilizando los valores de los parámetros proporcionados. Este método  
        /// consultara la base de datos para descubrir los parámetros para el procedimiento almacenado y asi asignara
        /// los valores basados en el orden de los parámetros.       
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, "PublishOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>
        /// No devuelve ningún conjunto de resultados y no toma ningún parámetro
        /// </remarks>
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Int64"/> que representa el numero de filas afectadas con el comando</returns>
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
        /// Ejecuta un SqlCommand contra un <see cref="System.Data.SqlClient.SqlTransaction"/> proveido
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// No devuelve ningún conjunto de resultados y no toma ningún parámetro
        /// </remarks>      
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <returns>Un <see cref="System.Int64"/> que representa el numero de filas afectadas con el comando</returns>
        public static int ExecuteNonQuery(SqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(transaction, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Ejecuta un SqlCommand contra un <see cref="System.Data.SqlClient.SqlTransaction"/> proveido
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>
        /// <remarks>
        /// No devuelve ningún conjunto de resultados y no toma ningún parámetro
        /// </remarks>         
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de SqlParameters usados para ejecutar el comando</param>
        /// <returns>Un <see cref="System.Int64"/> que representa el numero de filas afectadas con el comando</returns>
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
        /// Ejecutar un procedimiento almacenado mediante un SqlCommand contra la base de datos especificada 
        /// en la cadena de conexión utilizando los valores de los parámetros proporcionados. Este método  
        /// consultara la base de datos para descubrir los parámetros para el procedimiento almacenado y asi asignara
        /// los valores basados en el orden de los parámetros. 
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// int result;
        /// result =  ExecuteNonQuery(conn, trans, "PublishOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>
        /// No devuelve ningún conjunto de resultados y no toma ningún parámetro
        /// </remarks>           
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Int64"/> que representa el numero de filas afectadas con el comando</returns>
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
        /// Ejecuta un SqlCommand contra un SqlConnection proporcionado     
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// Devuelve un conjunto de resultados
        /// </remarks>     
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(connectionString, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Ejecuta un SqlCommand sobre la base de datos especificada en la cadena de conexion
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>
        /// <remarks>
        /// Devuelve un conjunto de resultados
        /// </remarks>     
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>      
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText, params SqlParameter[] commandParameters)
        {
            //crea y abre uncreate & open SqlConnection, y dispone de ella después de que haya terminado.
            using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();
                //llama al metodo de recarga para que tome la connexion en lugar de la Cadena de String.
                return ExecuteDataset(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Ejecutar un procedimiento almacenado mediante un SqlCommand contra la base de datos especificada 
        /// en la cadena de conexión utilizando los valores de los parámetros proporcionados. Este método  
        /// consultara la base de datos para descubrir los parámetros para el procedimiento almacenado y asi asignara
        /// los valores basados en el orden de los parámetros. 
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// DataSet ds =  ExecuteDataset(connString, "GetOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado
        /// </remarks>  
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>
        public static DataSet ExecuteDataset(string connectionString, string spName, params object[] parameterValues)
        {
            //si recibimos valores de los parámetros, tenemos que averiguar dónde se dirigen
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //tirar de los parámetros para este procedimiento almacenado desde la memoria caché de parámetros (o descubrirlos y poblar la memoria caché).
                SqlParameter[] commandParameters = ParameterCache.GetSpParameterSet(connectionString, spName);

                //Asigna los valores proveidos a los Parametros basado en el orden de los parametros
                AssignParameterValues(commandParameters, parameterValues);

                //llama al metodo sobrecargado que toma el arreglo de SqlParameters 
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //en otro caso solamente se llama al procedimiento sin parametros
            else
            {
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Ejecuta un SqlCommand contra un SqlConnection proporcionado     
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders");
        /// </code>
        /// </example>
        /// <remarks> 
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado. No toma ningun parametro
        /// </remarks>         
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>
        public static DataSet ExecuteDataset(SqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(connection, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Ejecuta un SqlCommand sobre una base de datos especificada en un SqlConnection proporcionado    
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>     
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>
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
            return ds;
        }

        /// <summary> 
        /// Ejecutar un procedimiento almacenado mediante un SqlCommand contra la base de datos especificada 
        /// en la cadena de conexión utilizando los valores de los parámetros proporcionados. Este método  
        /// consultara la base de datos para descubrir los parámetros para el procedimiento almacenado y asi asignara
        /// los valores basados en el orden de los parámetros. 
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// DataSet ds = ExecuteDataset(conn, "GetOrders", 24, 36)
        /// </code>
        /// </example>
        /// <remarks>
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado
        /// </remarks>         
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>
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
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders");
        /// </code>
        /// </example>
        /// <remarks>
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado
        /// </remarks>        
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>
        public static DataSet ExecuteDataset(SqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pasar parametros nulo a través de la llamada al metodo sobrecargado para proporcionar el conjunto de SqlParameters
            return ExecuteDataset(transaction, commandType, commandText, (SqlParameter[])null);
        }

        /// <summary>
        /// Ejecuta un SqlCommand sobre un <see cref="System.Data.SqlClient.SqlTransaction"/> proveido
        /// </summary>
        /// <example>
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </code>
        /// </example>
        /// <remarks>
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado
        /// </remarks>        
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>
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
        /// Para utilizar este metodo es necesario una implementacion parecida a lo siguiente:
        /// <code>
        /// DataSet ds = ExecuteDataset(trans, "GetOrders", 24, 36);
        /// </code>
        /// </example>
        /// <remarks>
        /// Este metodo no provee acceso a parametros de salida o al valor de retorno del procedimiento almacenado
        /// </remarks>   
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> valido sobre el cual sera ejecutado este comando</param>
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando, o null</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">an array of SqlParameters to be associated with the command or 'null' if no parameters are required</param>
        /// <param name="connectionOwnership">indicates whether the connection parameter was provided by the caller, or created by SqlHelper</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns><see cref="System.Data.SqlClient.SqlDataReader" /> contieniendo los resultados del comando ejecutado</returns>
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
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
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
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
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
        /// <param name="connectionString">Una cadena de conexion válida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
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
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
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
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">El procedimiento, consulta o T-SQL a ejecutar</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
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
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command using "FOR XML AUTO"</param>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
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
        /// <param name="connection">Un <see cref="System.Data.SqlClient.SqlConnection"/> en el cual sera ejecutado el comando</param>
        /// <param name="spName">the name of the stored procedure using "FOR XML AUTO"</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
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
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command using "FOR XML AUTO"</param>
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
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="commandType">El <see cref="System.Data.CommandType"/> (Procedimiento Almacenado, Consulta, etc)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <param name="commandParameters">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
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
        /// <param name="transaction">El <see cref="System.Data.SqlClient.SqlTransaction"/> sobre el cual sera ejecutado el comando</param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="parameterValues">Un arreglo de parametros de Objetos que seran asignados como parametros de entrada al SP</param>
        /// <returns>Un <see cref="System.Data.DataSet"/> conteniendo el resultado generado por el Comnado ejecutado</returns>
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