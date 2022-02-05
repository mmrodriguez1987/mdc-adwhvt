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
          
            mySMS = new SMS(gbl.CcnAzureCommunicationServices);

            //Instance the BillUsage Model
            BillUsage bug = new BillUsage(gbl.CcnDatawareHouse, testFileName);            
           
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("GlobalTestResult");
            
            //Validation: Get Bills Generated on wrong Fiscal Year
            dsResult = await bug.GetBillGeneratedOnWrongFiscalYear(gbl.StartDate, gbl.EndDate);   
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());



            //Validation: Get Bills Generated on weekend
            dsResult = await bug.GetBillsGeneratedOnWeekend(gbl.StartDate, gbl.EndDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());


            //Validation: Get Bills Generated on weekend
            dsResult = await bug.GetCountDistinctBillOnDataLoadOverTheMaxHistric(gbl.StartDate, gbl.EndDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());


            _log.LogInformation("End of the Global Test at: " + DateTime.Now.ToString());

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));           
        }
    }
}
