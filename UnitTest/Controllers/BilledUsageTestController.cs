using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
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
            finalResultDS = new DataSet();
            dsResult = new DataSet();
        }

        [HttpGet]
        public async Task<IActionResult> Get(DateTime startDate, DateTime endDate, Boolean sendSMSNotify, Boolean saveResult)
        {
            #region Controller Initialization
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");           
            startDate = startDate.Date.AddHours(10);
            endDate = endDate.Date.AddHours(10);
            mySMS = new SMS(gbl.CcnACS);            
            BillUsage bug = new BillUsage(gbl.CcnDTW, gbl.CcnCDC, gbl.CcnDVT);
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");

            //dont notify
            sendSMSNotify = false;
            #endregion

            #region Validation: Bills Generated on wrong Fiscal Year
            dsResult = await bug.BillGeneratedOnWrongFiscalYear(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            #endregion

            #region Validation: Bills Generated on weekend
            dsResult = await bug.BillsGeneratedOnWeekend(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            #endregion

            #region Validation: Bills Generated vs Max Historic
            dsResult = await bug.BillCountVsMaxHistoric(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            #endregion

            #region Validation: Bill Segment Count
            dsResult = await bug.BillSegmentCount(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            #endregion

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
        }
    }
}
