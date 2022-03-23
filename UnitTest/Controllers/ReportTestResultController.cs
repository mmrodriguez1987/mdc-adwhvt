using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.DataConversion;
using Tools.Documentation;
using Tools.Communication;
using UnitTest.Model.ValidationTest;
using System.Net.Http;
using Newtonsoft.Json;
using System.Text;
using System.Net.Http.Headers;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Author("Michael Rodriguez", "11/30/2021", "Controller for Test Incremental Load at Azure Data Factory")]
    public class ReportTestResultController : ControllerBase
    {
        private Global gbl;
       
        public ReportTestResultController(IConfiguration conf)
        {
             gbl = new Global(conf);          
        }

        [HttpGet]
        public async Task<IActionResult> Get(DateTime endDate)
        {
            try
            {
                TestResult test = new TestResult(gbl.CcnDVT);

                string JSONResult = await test.getTestResultJSONFormat(endDate);
                
                var json = JsonConvert.SerializeObject(JSONResult);
                var data = new StringContent(json, Encoding.UTF8, "application/json");

                var url = "https://prod-63.eastus2.logic.azure.com:443/workflows/041784ca92844421b84a0d170fce42a0/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=Bt5NXXiZSZ5Eof9Mm1X_3KQddA-0_AgXjaWI8q9Uvao";
                
                using var client = new HttpClient();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var response = await client.PostAsync(url, data);

                var result = await response.Content.ReadAsStringAsync();
                
                Console.WriteLine(result);              

                return base.Ok(JSONResult);
            }
            catch (Exception e)
            {
                return base.BadRequest(e.ToString());
            }
        }
    }
}
