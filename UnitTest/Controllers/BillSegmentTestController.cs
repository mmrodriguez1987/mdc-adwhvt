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
         
          
                mySMS = new SMS(gbl.CCN_ACS);

                BillSegment bst = new BillSegment(gbl.CCN_DTWH, gbl.CCN_CDC, testFileName);

                dsResult = Extensions.getResponseStructure("");
                finalResultDS = Extensions.getResponseStructure("GlobalTestResult");

                //Validation: Get Count of Bill Segment and comparei
                dsResult = await bst.GetCountBillSegment(gbl.startDate, gbl.endDate);
                finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);

            mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM[0], dsResult.Tables[0].Rows[0][11].ToString());
            mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM[1], dsResult.Tables[0].Rows[0][11].ToString());
            mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM[2], dsResult.Tables[0].Rows[0][11].ToString());

            _log.LogInformation("End of the Global Test at: " + DateTime.Now.ToString());

                return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
            
            
        }
    }
}
