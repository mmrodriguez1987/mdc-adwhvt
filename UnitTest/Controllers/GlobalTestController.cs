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
        private string testFileName;

        public GlobalTestController(IConfiguration conf, ILogger<GlobalTestController> log)
        {
            _conf = conf;
            _log = log;
            myDict = new DDictionary();
            finalResultDS  = new DataSet();
            dsResult = new DataSet();
            testFileName = @"C:\ADW_UT\UT_BI_ADWH_" + DateTime.UtcNow.ToString("yyyy_MM_dd");
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
            _endDate = DateTime.UtcNow;
            _startDate = _endDate.AddDays(-1);
            _log.LogInformation("Begin the Global Test at: " + _endDate.ToString() + " \nEvaluated Date Range: StartDate => " + _startDate + " EndDate => " + _endDate);

            _log.LogInformation("DTW Con: " + _conf.GetConnectionString("DTWttdpConnection"));
            _log.LogInformation("CDC Con: " + _conf.GetConnectionString("CDCProdConnection"));

            BillUsage bu = new BillUsage(_conf.GetConnectionString("DTWttdpConnection"),testFileName);
            Account acct = new Account(_conf.GetConnectionString("DTWttdpConnection"), _conf.GetConnectionString("CDCProdConnection"),testFileName);

            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("GlobalTestResult");

            dsResult = await bu.GetBillGeneratedOnWrongFiscalYear(_startDate, _endDate);    
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            
            dsResult = await bu.GetBillsGeneratedOnWeekend(_startDate, _endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
                     

            dsResult = await acct.GetCountAccounts(_startDate, _endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);

            _log.LogInformation("End of the Global Test at: " + DateTime.Now.ToString());
            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
        }
    }
}
