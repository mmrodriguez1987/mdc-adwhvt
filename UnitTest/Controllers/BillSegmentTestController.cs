using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.DataConversion;
using Tools.Documentation;
using Tools.Communication;
using Tools.Mechanism;
using UnitTest.Model.DataWarehouse;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Author("Michael Rodriguez", "11/30/2021", "Controller for Test Incremental Load at Azure Data Factory")]
    public class BillSegmentTestController : ControllerBase
    {
            
        private readonly ILogger<BillSegmentTestController> _log;     
        private DataSet finalResultDS, dsResult;
        private string testFileName;
        private SMS mySMS;
        private Global gbl;

        public BillSegmentTestController(IConfiguration conf, ILogger<BillSegmentTestController> log)
        {
            gbl = new Global(conf);
            _log = log;           
            finalResultDS = new DataSet();
            dsResult = new DataSet();
            testFileName = @gbl.LogFileRoot + "UT_BI_ADWH_" + DateTime.UtcNow.ToString("yyyy_MM_dd");
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
            mySMS = new SMS(gbl.CcnAzureCommunicSrvs);
            BillSegment bst = new BillSegment(gbl.CcnDatawareHouse, gbl.CcnCDC, testFileName);

            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("GlobalTestResult");

            //Validation: Get Count of Bill Segment and comparei
            dsResult = await bst.UniqueBillSegmentCount(gbl.StartDate, gbl.EndDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());



            _log.LogInformation("End of the Global Test at: " + DateTime.Now.ToString());

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));           
            
        }
    }
}
