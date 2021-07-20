using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace UnitTest.Models
{
    public class UnitTest
    {       

        private static SqlConnection myConnection;
        private static SqlCommand command;
        private static SqlDataAdapter dataAdapter;
        private static DataSet resultDTWH = new DataSet();
        private static string sql_query;

        private static OracleConnection myOracleConnection = null;
        private static OracleCommand commandOracle = null;
        private static OracleDataAdapter dataAdapterOracle = null;
        private static DataSet resultsOracle = new DataSet();
        private static string oracle_query;

        private static string _oracleConex, _dtwhConex, _dtwTable, _ccbTable;

       
        public static string DtwTable { get => _dtwTable; set => _dtwTable = value; }
        public static string CcbTable { get => _ccbTable; set => _ccbTable = value; }

        public static DataSet setResponseStructure()
        {
            DataSet dsResult = new DataSet("dsResults");
            DataTable dtResult = new DataTable("dtResult");
            dtResult.Columns.Add("DWH");
            dtResult.Columns.Add("Oracle");
            dtResult.Columns.Add("result");
            dtResult.Columns.Add("Error");
            dtResult.Columns.Add("Diff");
            dtResult.Rows.Add(0, 0, 0, "", 0);
            dsResult.Tables.Add(dtResult);
            return dsResult;
        }       

        public UnitTest(string ccbConex, string dtwhConex, string identifier)
        {
            _oracleConex = ccbConex;
            _dtwhConex = dtwhConex;
            setTablesByIdentifier(identifier);
         

            oracle_query = "SELECT COUNT(*) as count FROM " + _ccbTable;
            sql_query = "SELECT COUNT(*) as count FROM " + _dtwTable;

            resultsOracle = setResponseStructure();
            resultDTWH = setResponseStructure();
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
                    myDS = setResponseStructure();
                    // Return -1 in Colum "OK" and the Error description in "Any Error" column
                    Console.Write("Conexion DTW: " + _dtwhConex + "\n");
                    Console.Write("Exception: " + dte + "\n");
                    myDS.Tables[0].Rows[0][0] = -1;
                    myDS.Tables[0].Rows[0][0] += (" Error Detail: " + dte.ToString());
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
                    myDS = setResponseStructure();
                    // Return -1 in Colum "OK" and the Error description in "Any Error" column
                    Console.Write("Conexion CCB: " + _oracleConex + "\n");
                    Console.Write("Exception: " + ccbe);                
                    myDS.Tables[0].Rows[0][0] = -1;
                    myDS.Tables[0].Rows[0][0] += (" Exception Detail: " + ccbe.ToString());                    
                    return myDS;
                }              
            });
        }

        /// <summary>
        /// This method is for Identify the two tables for the Unit Test
        /// </summary>
        /// <param name="tableIdentifier"></param>
        public static void setTablesByIdentifier(string tableIdentifier)
        {           
            switch (tableIdentifier.Trim().ToUpper())
            {
                case "UOM":
                    _dtwTable = "dwadm2.CD_UOM";
                    _ccbTable = "CISADM.CI_UOM";
                    break;
                case "FISCAL_CAL":
                case "CAL_PERIOD":
                    _dtwTable = "dwadm2.CD_FISCAL_CAL";
                    _ccbTable = "CISADM.CI_CAL_PERIOD";
                    break;

                case "ACCT":
                    _dtwTable = "dwadm2.CD_ACCT";
                    _ccbTable = "CISADM.CI_ACCT";
                    break;

                case "ADDR":
                    _dtwTable = "dwadm2.CD_ADDR";
                    _ccbTable = "CISADM.CI_PREM";
                    break;

                case "PREM":
                    _dtwTable = "dwadm2.CD_PREM";
                    _ccbTable = "CISADM.CI_PREM";
                    break;

                case "RATE":
                case "RS":
                    _dtwTable = "dwadm2.CD_RATE";
                    _ccbTable = "CISADM.CI_RS";
                    break;

                case "SA":
                    _dtwTable = "dwadm2.CD_SA";
                    _ccbTable = "CISADM.CI_SA";
                    break;

                case "PER":
                case "PERSON":
                    _dtwTable = "dwadm2.CD_PER";
                    _ccbTable = "CISADM.CI_PER";
                    break;

                case "SQI":
                    _dtwTable = "dwadm2.CD_SQI";
                    _ccbTable = "CISADM.CI_SQI";
                    break;

                default:
                    _dtwTable = "";
                    _ccbTable = "";
                    break;
            }

        }

       
    }
}
