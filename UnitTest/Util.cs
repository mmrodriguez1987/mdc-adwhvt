using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace UnitTest
{
    public class Util
    {

        public static SqlConnection myConnection;
        public static SqlCommand command;
        public static SqlDataAdapter dataAdapter;
        public static DataSet resultDTWH = new DataSet();
        public static string sql_query;


        public static OracleConnection myOracleConnection = null;
        public static OracleCommand commandOracle = null;
        public static OracleDataAdapter dataAdapterOracle = null;
        public static DataSet resultsOracle = new DataSet();
        public static string oracle_query;

        public static string _oracleConex, _dtwhConex, _dtwTable, _ccbTable;
    
        public static DataSet setDSDefaultStructure(string ccb, string dwt)
        {
            DataSet dsResult = new DataSet("dsResults");
            DataTable dtResult = new DataTable("dtResult");
            dtResult.Columns.Add(dwt);
            dtResult.Columns.Add(ccb);
            dtResult.Columns.Add("result");
            dtResult.Columns.Add("Error");
            dtResult.Rows.Add(0, 0, 0, "");
            dsResult.Tables.Add(dtResult);
            return dsResult;
        }       

        public Util(string oracleConex, string dtwhConex, string dtwTable, string ccbTable)
        {
            _oracleConex = oracleConex;
            _dtwhConex = dtwhConex;
            _dtwTable = dtwTable;
            _ccbTable = ccbTable;

            oracle_query = "SELECT COUNT(*) as count FROM " + _ccbTable;
            sql_query = "SELECT COUNT(*) as count FROM " + _dtwTable;

            resultsOracle = setDSDefaultStructure(ccbTable, dtwTable);
            resultDTWH = setDSDefaultStructure(ccbTable, dtwTable);
        }
    
        /// <summary>
        /// Convert a DataTabla into Json
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static string DataTableToJSONWithStringBuilder(DataTable table)
        {
            var JSONString = new StringBuilder();
            if (table.Rows.Count > 0)
            {
                //JSONString.Append("[");
                for (int i = 0; i < table.Rows.Count; i++)
                {
                    JSONString.Append("{");
                    for (int j = 0; j < table.Columns.Count; j++)
                    {
                        if (j < table.Columns.Count - 1)
                        {
                            JSONString.Append("\"" + table.Columns[j].ColumnName.ToString() + "\":" + "\"" + table.Rows[i][j].ToString() + "\",");
                        }
                        else if (j == table.Columns.Count - 1)
                        {
                            JSONString.Append("\"" + table.Columns[j].ColumnName.ToString() + "\":" + "\"" + table.Rows[i][j].ToString() + "\"");
                        }
                    }
                    if (i == table.Rows.Count - 1)
                    {
                        JSONString.Append("}");
                    }
                    else
                    {
                        JSONString.Append("},");
                    }
                }
                //JSONString.Append("]");
            }
            return JSONString.ToString();
        }

        /// <summary>
        /// Get Rowws Count from a specified table of Datawarehouse in a conext of an object in th instance
        /// </summary>
        /// <returns>A dataset with data</returns>
        public static Task<DataSet> GetRowsCountFromDTWH()
        {
            DataSet myDS = new DataSet();
            return Task.Run(() =>
            {                
            try
                {                   
                    //Conexion to Datawarehouse
                    using (myConnection = new SqlConnection(_dtwhConex))
                    {
                        myConnection.Open();
                        using (command = new SqlCommand(sql_query, myConnection))
                        {
                            using (dataAdapter = new SqlDataAdapter(command))
                            {
                                //dataAdapter.SelectCommand.CommandType = CommandType.Text;
                                dataAdapter.Fill(myDS);
                                myConnection.Close();                               
                                return myDS;
                            }
                        }
                    }
                }
                catch (Exception dte)
                {
                    myDS = setDSDefaultStructure("error", "error");
                    // Return -1 in Colum "OK" and the Error description in "Any Error" column
                    Console.Write("Conexion DTW: " + _dtwhConex + "\n");
                    Console.Write("Exception: " + dte + "\n");
                    myDS.Tables[0].Rows[0][0] = -1;
                    myDS.Tables[0].Rows[0][0] += ("Exception: " + dte.ToString());
                    return myDS;
                }
            });           
        }

        /// <summary>
        /// /// Get Rowws Count from a specified table of CCB in a conext of an object in th instance
        /// </summary>
        /// <returns>A dataset with data</returns>
        public static Task<DataSet> GetRowsCountFromCCB()
        {
            DataSet myDS = new DataSet();
            return Task.Run(() =>
            {
                try
                {                  
                    using (myOracleConnection = new OracleConnection(_oracleConex))
                    {
                        myOracleConnection.Open();
                        using (commandOracle = new OracleCommand(oracle_query, myOracleConnection))
                        {
                            using (dataAdapterOracle = new OracleDataAdapter(commandOracle))
                            {
                                //dataAdapterOracle.SelectCommand.CommandType = CommandType.Text;
                                dataAdapterOracle.Fill(myDS);
                                myOracleConnection.Close();
                                return myDS;                               
                            }
                        }
                    }
                }
                catch (Exception ccbe)
                {
                    myDS = setDSDefaultStructure("error","error");
                    // Return -1 in Colum "OK" and the Error description in "Any Error" column
                    Console.Write("Conexion CCB: " + _oracleConex + "\n");
                    Console.Write("Exception: " + ccbe);
                    myDS.Tables[0].Rows[0][0] = -1;
                    myDS.Tables[0].Rows[0][0] += ("Exception: " + ccbe.ToString());
                    return myDS;
                }              
            });
        }
    }
}
