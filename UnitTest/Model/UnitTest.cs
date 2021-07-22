using System.Threading;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

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
        readonly ILogger<UnitTest> _log;
        private static string _oracleConex, _dtwhConex, _dtwTable, _ccbTable;
       
        public string DtwTable { get => _dtwTable; set => _dtwTable = value; }
        public string CcbTable { get => _ccbTable; set => _ccbTable = value; }

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
                int i = 0;
               
                using (myConnection = new SqlConnection(_dtwhConex))
                {
                    //Do-While for try to connect 3 times on the database
                    do
                    {
                        try
                        {
                            myConnection.Open();
                            if (myConnection.State == ConnectionState.Open) break; else i++; //if connection is open exit of boucle else keep                          
                        }
                        catch (Exception e)
                        {
                            i++;
                            Console.Write("Error trying to connect to DTWH, attempt #" + (i + 1));
                            Console.Write("Exception: " + e.ToString());
                            Thread.Sleep(3000);
                            if (i == 3)
                            {
                                myDS = setResponseStructure();                                
                                myDS.Tables[0].Rows[0][0] = ("DTWH, There is a Conexion Exception, 3 attemp were made");
                                return myDS;
                            }
                        }
                    }
                    while (i <= 3);

                    try
                    {
                        using (command = new SqlCommand(sql_query, myConnection))
                        {
                            using (dataAdapter = new SqlDataAdapter(command))
                            {                                
                                dataAdapter.Fill(myDS);
                                myConnection.Close();
                                return myDS;
                            }
                        }
                    }
                    catch(Exception b)
                    {
                        Console.Write("Error executing the command");
                        Console.Write("Exception Details: " + b.ToString());
                        myDS.Tables[0].Rows[0][0] = ("Error executing the command on database");
                        return myDS;
                    }                               
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
                int i = 0;
                                
                using (myOracleConnection = new OracleConnection(_oracleConex))
                {   
                    //Do-While for try to connect 3 times on the database
                    do
                    {
                        try
                        {
                            myOracleConnection.Open();
                            if (myOracleConnection.State == ConnectionState.Open) break; else i++; //if connection is open exit of boucle else keep                          
                        } 
                        catch(Exception err)
                        {
                            i++;
                            Console.Write("Error trying to connect to CCB, attempt #" + (i+1));
                            Console.Write("Exception: " + err.ToString());
                            Thread.Sleep(3000);
                            if (i == 3)
                            {                                
                                myDS = setResponseStructure();                          
                                myDS.Tables[0].Rows[0][0] = ("CCB, There is a Conexion Error , 3 attemp were made");
                                return myDS;
                            }   
                        }
                    } while (i  <=  3);

                      
                    try
                    {
                        using (commandOracle = new OracleCommand(oracle_query, myOracleConnection))
                        {
                            using (dataAdapterOracle = new OracleDataAdapter(commandOracle))
                            {
                                dataAdapterOracle.Fill(myDS);
                                myOracleConnection.Close();
                                return myDS;
                            }
                        }
                    }
                    catch(Exception e)
                    {
                        Console.Write("Error executing the command");
                        Console.Write("Exception Details: " + e.ToString());   
                        myDS.Tables[0].Rows[0][0] = (" Error executing the command, exception Detail: ");
                        return myDS;                        
                    }                   
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
