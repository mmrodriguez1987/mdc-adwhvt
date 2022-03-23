using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Net.Http.Formatting;

namespace ValidationTest
{
    internal class Program
    {
        static DateTime startDate, endDate;
        static String? httpResponseAPI = String.Empty, serverURI = String.Empty, keyTestList = String.Empty, startHour, endHour;
        static List<String> testList = new List<String>();
        static Boolean keySMSNotify, keySaveResult;

        static void Main(String[] args)
        {                   
            serverURI = Convert.ToString(ConfigurationManager.AppSettings["Host"]);
            keyTestList = Convert.ToString(ConfigurationManager.AppSettings["TestList"]);
            keySMSNotify = Convert.ToBoolean(ConfigurationManager.AppSettings["sendSMSNotify"]);
            keySaveResult = Convert.ToBoolean(ConfigurationManager.AppSettings["saveResult"]);
            startHour = Convert.ToString(ConfigurationManager.AppSettings["startHour"]);
            endHour = Convert.ToString(ConfigurationManager.AppSettings["endHour"]);

            if (keyTestList is not null) 
                testList = keyTestList.Split(",").ToList<String>();
            
            
            startDate = DateTime.Now.Date.AddDays(-1);
            endDate = DateTime.Now.Date;
            string test = "https://google.com.ni/";
            Console.WriteLine();

            foreach (string item in testList) {
                string url = "api/" + item + "?startDate=" + startDate.ToString("yyyy-MM-dd") + "&endDate=" + endDate.ToString("yyyy-MM-dd") + "&sendSMSNotify=" + (keySMSNotify  ? "true" : "false") + "&saveResult=" + (keySaveResult ? "true" : "false") + "&starHour=" + startHour + "&endHour=" + endHour;
                if (serverURI is not null)
                {                   
                    Console.WriteLine("Server URL: " + serverURI + ", url: " + url);
                    CallWebAPIAsync(serverURI, url).Wait();
                    Thread.Sleep(3000);
                }                                        
            }                    
               
        } 
          

        
        private static async Task CallWebAPIAsync(String _uri, String _request)
        {             
            using (var client = new HttpClient())
            {
                try
                {
                    client.BaseAddress = new Uri(_uri);
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    //GET Method
                    HttpResponseMessage response = await client.GetAsync(_request);
                    if (response.IsSuccessStatusCode)
                    {
                        httpResponseAPI = await response.Content.ReadAsStringAsync();
                        Console.WriteLine(httpResponseAPI);
                    }
                    else
                        Console.WriteLine("Error: Status Code: " + response.StatusCode.ToString() + " , Description: " + response.ReasonPhrase.ToString());
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error calling the API: " + ex.ToString());
                }     
            }
        }
    }
}
