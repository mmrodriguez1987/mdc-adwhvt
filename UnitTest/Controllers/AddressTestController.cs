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
    public class AddressTestController : ControllerBase
    {
        private DataSet finalResultDS, dsResult;
        private SMS mySMS;
        private Global gbl;

        public AddressTestController(IConfiguration conf)
        {
            gbl = new Global(conf);
            finalResultDS = new DataSet();
            dsResult = new DataSet();

        }

        [HttpGet]
        public async Task<IActionResult> Get(DateTime startDate, DateTime endDate, Boolean sendSMSNotify, Boolean saveResult)
        {
            #region Controller Initializacion
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");
            startDate = startDate.Date.AddHours(10);
            endDate = endDate.Date.AddHours(10);
            mySMS = new SMS(gbl.CcnACS);
            Address addr = new Address(gbl.CcnDTW, gbl.CcnCDC, gbl.CcnDVT);
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");
            #endregion

            #region Validation: Premise Count
            dsResult = await addr.AddressCount(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            if (dsResult.Tables[0].Rows[0][11].ToString().StartsWith("Error"))
                return base.BadRequest(dsResult.Tables[0].Rows[0][11].ToString());
            else
            {
                if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            }
            #endregion

            #region Validation: New Premise Count
            dsResult = await addr.NewAddressCount(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            if (dsResult.Tables[0].Rows[0][11].ToString().StartsWith("Error"))
                return base.BadRequest(dsResult.Tables[0].Rows[0][11].ToString());
            else
            {
                if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            }
            #endregion

            #region Validation: Updated Premises Count
            dsResult = await addr.UpdatedAddressCount(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            if (dsResult.Tables[0].Rows[0][11].ToString().StartsWith("Error"))
                return base.BadRequest(dsResult.Tables[0].Rows[0][11].ToString());
            else
            {
                if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            }
            #endregion

            #region Validation: Statistical comparision of account
            dsResult = await addr.StatisticalAddressEvaluation(endDate, gbl.EvaluatedDatesRangeOnAverageTest, gbl.ToleranceVariatonNumber, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);

            if (dsResult.Tables[0].Rows[0][11].ToString().StartsWith("Error"))
                return base.BadRequest(dsResult.Tables[0].Rows[0][11].ToString());
            else
            {
                if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            }
            #endregion

            #region Validation: Premises Count vs Max Historic
            dsResult = await addr.AddressCountVsMaxHist(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][11].ToString().StartsWith("Error"))
                return base.BadRequest(dsResult.Tables[0].Rows[0][11].ToString());
            #endregion

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
        }
    }
}
