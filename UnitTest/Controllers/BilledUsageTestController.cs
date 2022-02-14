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
    public class BilledUsageTestController : ControllerBase
    {
        private IConfiguration _conf { get; }      
        private DataSet finalResultDS, dsResult;       
        private SMS mySMS;
        private Global gbl;

        public BilledUsageTestController(IConfiguration conf)
        {
            gbl = new Global(conf);                   
            finalResultDS  = new DataSet();            
            dsResult = new DataSet();
           
        }

        [HttpGet]
        public async Task<IActionResult> Get(DateTime startDate, DateTime endDate, Boolean saveResult)
        {
            //Local members
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");

            //Adding 10am to the hour
            startDate = startDate.Date.AddHours(10);
            endDate = endDate.Date.AddHours(10);

            mySMS = new SMS(gbl.CcnACS);

            //Instance the BillUsage Model
            BillUsage bug = new BillUsage(gbl.CcnDTW,gbl.CcnCDC, gbl.CcnDVT);            
           
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("GlobalTestResult");
            
            //Validation: Get Bills Generated on wrong Fiscal Year
            dsResult = await bug.GetBillGeneratedOnWrongFiscalYear(startDate, endDate, saveResult);   
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());


            //Validation: Get Bills Generated on weekend
            dsResult = await bug.GetBillsGeneratedOnWeekend(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            //Validation: Get Bills Generated vs Max Historic

            
            dsResult = await bug.GetCountDistinctBillOnDataLoadOverTheMaxHistric(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
                        

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));           
        }
    }
}
