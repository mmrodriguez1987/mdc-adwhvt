using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.DataConversion;
using Tools.Documentation;
using Tools.Mechanism;
using UnitTest.Model.DataWarehouse;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Author("Michael Rodriguez", "11/30/2021", "Controller for Test Incremental Load at Azure Data Factory")]
    public class GlobalTestController : ControllerBase
    {
        private IConfiguration _conf { get; }
        private static DDictionary myDict;
        private readonly ILogger<GlobalTestController> _log;
        private DateTime _startDate, _endDate;
        private DataSet finalResultDS, dsResult;

        public GlobalTestController(IConfiguration conf, ILogger<GlobalTestController> log)
        {
            _conf = conf;
            _log = log;
            myDict = new DDictionary();
            finalResultDS  = new DataSet();
            dsResult = new DataSet();
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {          
            _startDate = DateTime.Today;
            _endDate = _startDate.AddDays(1);
            _log.LogInformation("Begin the Global Test");


            BillUsage bu = new BillUsage(_conf.GetConnectionString("DTWttdpConnection"));
            dsResult = bu.getResponseStructure("");
            finalResultDS = bu.getResponseStructure("GlobalTestResult");

            dsResult = await bu.GetBillGeneratedOnWrongFiscalYear(_startDate, _endDate);    
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            
            dsResult = await bu.GetBillsGeneratedOnWeekend(_startDate, _endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);


            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
        }
    }
}
