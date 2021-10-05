using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;

namespace DBHelper
{
    public class ParameterCache
    {

        /// <summary>
        /// Objeto sincronizado de acceso privado (seguro para la ejecución de subprocesos)
        /// <seealso href="http://msdn.microsoft.com/es-es/library/system.collections.hashtable.synchronized%28v=vs.90%29.aspx">Hashtable.Synchronized (Método)</seealso>
        /// </summary>
        private static Hashtable paramCache = Hashtable.Synchronized(new Hashtable());

        #region private methods, variables, and constructors


        /// <summary>
        /// El constructor por defecto es privado porque todos los metodos de la clase son estaticos
        /// </summary>
        private ParameterCache()
        {

        }

        /// <summary>
        /// Resuelve en tiempo de ejecucuion un conjunto de SqlParameter para un Procedimiento Almacenado
        /// </summary>
        /// <param name="connectionString">Una cadena de Conexion (<see cref="System.String"/>) valida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">El nombre del Procedimiento almacenado</param>
        /// <param name="includeReturnValueParameter">Si se incluye o no el parametro utilizado como valor de retorno</param>
        /// <returns>Devuelve un arreglo de parametros</returns>
        private static SqlParameter[] DiscoverSpParameterSet(string connectionString, string spName, bool includeReturnValueParameter)
        {
            using (SqlConnection cn = new SqlConnection(connectionString))
            using (SqlCommand cmd = new SqlCommand(spName, cn))
            {
                cn.Open();
                cmd.CommandType = CommandType.StoredProcedure;
                SqlCommandBuilder.DeriveParameters(cmd);
                if (!includeReturnValueParameter)
                {
                    cmd.Parameters.RemoveAt(0);
                }
                SqlParameter[] discoveredParameters = new SqlParameter[cmd.Parameters.Count];
                cmd.Parameters.CopyTo(discoveredParameters, 0);
                return discoveredParameters;
            }
        }

        /// <summary>
        /// Deep copy of cached SqlParameter array
        /// </summary>
        /// <param name="originalParameters">Arreglo de Parametros que se desea clonar</param>
        /// <returns>Arreglo de Parametros clonados</returns>
        private static SqlParameter[] CloneParameters(SqlParameter[] originalParameters)
        {
            SqlParameter[] clonedParameters = new SqlParameter[originalParameters.Length];
            for (int i = 0, j = originalParameters.Length; i < j; i++)
            {
                clonedParameters[i] = (SqlParameter)((ICloneable)originalParameters[i]).Clone();
            }
            return clonedParameters;
        }
        #endregion private methods, variables, and constructors

        #region caching functions

        /// <summary>       
        /// Agrega un arreglo de parametros a la Cache
        /// </summary>
        /// <param name="connectionString">Una cadena de Conexion (<see cref="System.String"/>) valida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandText">Un <see cref="System.String"/> que representa el Procedimiento Almacenado o el comando T-SQL</param>
        /// <param name="commandParameters">Un arreglo de SqlParameter para ser puesto en la Cache</param>
        public static void CacheParameterSet(string connectionString, string commandText, params SqlParameter[] commandParameters)
        {
            string hashKey = connectionString + ":" + commandText;
            paramCache[hashKey] = commandParameters;
        }

        /// <summary>
        /// Retraer un arreglo de parametros desde la cache
        /// </summary>
        /// <param name="connectionString">Una cadena de Conexion (<see cref="System.String"/>) valida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="commandText">Un <see cref="System.String"/> que representa el Procedimiento Almacenado o el comando T-SQL</param>
        /// <returns>Un Arreglo de SqlParameter </returns>
        public static SqlParameter[] GetCachedParameterSet(string connectionString, string commandText)
        {
            string hashKey = connectionString + ":" + commandText;
            SqlParameter[] cachedParameters = (SqlParameter[])paramCache[hashKey];
            if (cachedParameters == null)
            {
                return null;
            }
            else
            {
                return CloneParameters(cachedParameters);
            }
        }
        #endregion caching functions

        #region Parameter Discovery Functions

        /// <summary>
        /// Trae un conjunto de SqlParameter apropiados para el procedimiento almacenado
        /// </summary>
        /// <remarks>
        /// This method will query the database for this information, and then store it in a cache for future requests.
        /// </remarks>
        /// <param name="connectionString">Una cadena de Conexion (<see cref="System.String"/>) valida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <returns>an array of SqlParameters</returns>
        public static SqlParameter[] GetSpParameterSet(string connectionString, string spName)
        {
            return GetSpParameterSet(connectionString, spName, false);
        }

        /// <summary>
        /// Retrieves the set of SqlParameters appropriate for the stored procedure
        /// </summary>
        /// <remarks>
        /// This method will query the database for this information, and then store it in a cache for future requests.
        /// </remarks>
        /// <param name="connectionString">Una cadena de Conexion (<see cref="System.String"/>) valida para <see cref="System.Data.SqlClient.SqlConnection"/></param>
        /// <param name="spName">Nombre del Procedimiento Almacenados</param>
        /// <param name="includeReturnValueParameter">a bool value indicating whether the return value parameter should be included in the results</param>
        /// <returns>an array of SqlParameters</returns>
        public static SqlParameter[] GetSpParameterSet(string connectionString, string spName, bool includeReturnValueParameter)
        {
            string hashKey = connectionString + ":" + spName + (includeReturnValueParameter ? ":include ReturnValue Parameter" : "");

            SqlParameter[] cachedParameters;
            cachedParameters = (SqlParameter[])paramCache[hashKey];
            if (cachedParameters == null)
            {
                cachedParameters = (SqlParameter[])(paramCache[hashKey] = DiscoverSpParameterSet(connectionString, spName, includeReturnValueParameter));
            }
            return CloneParameters(cachedParameters);
        }
        #endregion Parameter Discovery Functions

    }
}

