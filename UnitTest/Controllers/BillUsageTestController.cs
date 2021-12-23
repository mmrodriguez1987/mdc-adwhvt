using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.DataConversion;
using Tools.Documentation;
using Tools.Communication;
using UnitTest.Model.DataWarehouse;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Author("Michael Rodriguez", "11/30/2021", "Controller for Test Incremental Load at Azure Data Factory")]
    public class BillUsageTestController : ControllerBase
    {
        private IConfiguration _conf { get; }        
        private readonly ILogger<BillUsageTestController> _log;      
        private DataSet finalResultDS, dsResult;
        private string testFileName;
        private SMS mySMS;
        private Global gbl;

        public BillUsageTestController(IConfiguration conf, ILogger<BillUsageTestController> log)
        {
            gbl = new Global(conf);
            _log = log;           
            finalResultDS  = new DataSet();            
            dsResult = new DataSet();
            testFileName = @gbl.LogFileRoot + "UT_BI_ADWH_" + DateTime.UtcNow.ToString("yyyy_MM_dd");
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
          
            mySMS = new SMS(gbl.CCN_ACS);

            //Instance the BillUsage Model
            BillUsage bug = new BillUsage(gbl.CCN_DTWH, testFileName);            
           
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("GlobalTestResult");
            
            //Validation: Get Bills Generated on wrong Fiscal Year
            dsResult = await bug.GetBillGeneratedOnWrongFiscalYear(gbl.startDate, gbl.endDate);   
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM[0], dsResult.Tables[0].Rows[0][11].ToString());
            mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM[1], dsResult.Tables[0].Rows[0][11].ToString());
            mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM[2], dsResult.Tables[0].Rows[0][11].ToString());

            //Validation: Get Bills Generated on weekend
            dsResult = await bug.GetBillsGeneratedOnWeekend(gbl.startDate, gbl.endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            //Send SMS Notification
            mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM[0], dsResult.Tables[0].Rows[0][11].ToString());
            mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM[1], dsResult.Tables[0].Rows[0][11].ToString());
            mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM[2], dsResult.Tables[0].Rows[0][11].ToString());

            _log.LogInformation("End of the Global Test at: " + DateTime.Now.ToString());

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));           
        }
    }
}
