using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace UnitTest.Model
{
    public class EnviromentInformation
    {
        private string _ccn;
        private static SqlConnection myConnection;
        private static SqlCommand command;
        private static SqlDataAdapter dataAdapter;
        private static DataSet resultDTWH = new DataSet();
        private static string sql_query;

        public EnviromentInformation()
        {
                                
        }
        public Task<Boolean> GetInformationTest(string _ccn)
        {
           
            return Task.Run(() =>
            {
                int i = 0;
                Boolean r = false;

                using (myConnection = new SqlConnection(_ccn))
                {                    
                    do
                    {
                        try
                        {
                            myConnection.Open();
                            if (myConnection.State == ConnectionState.Open) {
                                r = true;
                                break;
                            }  else i++; //if connection is open exit of boucle else keep                          
                        }
                        catch (Exception e)
                        {
                            i++;                           
                            Thread.Sleep(1000);
                            if (i == 3)                            
                                r = false;                                 
                            
                        }
                    }
                    while (i <= 3);
                    return r;
                }
            });
        }
    }
}
