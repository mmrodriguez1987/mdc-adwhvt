using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.DataConversion;
using Tools.Documentation;
using Tools.Communication;
using UnitTest.Model.DataWarehouse;
using UnitTest.Model.ValidationTest;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Author("Michael Rodriguez", "11/30/2021", "Controller for Test Incremental Load at Azure Data Factory")]
    public class ServiceQuantityIdentifierTestController : ControllerBase
    {    
        private DataSet finalResultDS, dsResult;       
        private SMS mySMS;
        private Global gbl;
        private TestResult ts;

        public ServiceQuantityIdentifierTestController(IConfiguration conf)
        {
            gbl = new Global(conf);              
            finalResultDS = new DataSet();
            dsResult = new DataSet();
            ts = new TestResult();
            ts.CcnAzureTables = gbl.UrlAzureTables;
        }

        [HttpGet]
        public async Task<IActionResult> Get(DateTime startDate, DateTime endDate, Boolean sendSMSNotify, Boolean saveResult, Int32 startHour, Int32 endHour)
        {
            #region Controller Initialization   
            //Local members
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");
            startDate = startDate.Date.AddHours((startHour == 0) ? gbl.StartHour : startHour);
            endDate = endDate.Date.AddHours((endHour == 0) ? gbl.EndHour : endHour);
            mySMS = new SMS(gbl.CcnACS);
            ServiceQuantityIdentifier sqi = new ServiceQuantityIdentifier(gbl.CcnDTW, gbl.CcnCDC, gbl.CcnDVT);
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");
            #endregion

            #region Validation: SQI Count     
            dsResult = await sqi.SQICount(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
           if (dsResult.Tables[0].Rows[0][2].ToString().StartsWith("Error"))
            {
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers[0], dsResult.Tables[0].Rows[0][2].ToString());
                return base.BadRequest(dsResult.Tables[0].Rows[0][2].ToString());
            }
            else
            {
                if ((Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 1 || Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 2) && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][2].ToString());
            }
            #endregion

            #region Validation New SQI
            dsResult = await sqi.NewSQICount(startDate,endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
           if (dsResult.Tables[0].Rows[0][2].ToString().StartsWith("Error"))
            {
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers[0], dsResult.Tables[0].Rows[0][2].ToString());
                return base.BadRequest(dsResult.Tables[0].Rows[0][2].ToString());
            }
            else
            {
                if ((Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 1 || Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 2) && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][2].ToString());
            }
            #endregion

            #region Validation: Updated SQI
            dsResult = await sqi.UpdatedSQICount(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
           if (dsResult.Tables[0].Rows[0][2].ToString().StartsWith("Error"))
            {
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers[0], dsResult.Tables[0].Rows[0][2].ToString());
                return base.BadRequest(dsResult.Tables[0].Rows[0][2].ToString());
            }
            else
            {
                if ((Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 1 || Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 2) && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][2].ToString());
            }
            #endregion

            #region Validation: Statistical comparision
            dsResult = await sqi.StatisticalSQIEvaluation(endDate, gbl.EvaluatedDatesRangeOnAverageTest, gbl.ToleranceVariatonNumber);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);

           if (dsResult.Tables[0].Rows[0][2].ToString().StartsWith("Error"))
            {
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers[0], dsResult.Tables[0].Rows[0][2].ToString());
                return base.BadRequest(dsResult.Tables[0].Rows[0][2].ToString());
            }
            else
            {
                if ((Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 1 || Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 2) && false)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][2].ToString());
            }
            #endregion
            /*
            #region Validation: SQI Count vs Max Historic
            dsResult = await sqi.SQICountVsMaxHistoric(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
           if (dsResult.Tables[0].Rows[0][2].ToString().StartsWith("Error"))
            {
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers[0], dsResult.Tables[0].Rows[0][2].ToString());
                return base.BadRequest(dsResult.Tables[0].Rows[0][2].ToString());
            }
            else
            {
               if ((Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 1 || Convert.ToInt32(dsResult.Tables[0].Rows[0][0]) == 2) && false)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][2].ToString());
            }
            #endregion
            */
            string jsonRes = ts.getTestResultJSONFormat(finalResultDS);

            if (saveResult)
            {
                string response = await ts.recordValidationOnAzureStorage(jsonRes);

                if (response != "OK")
                    return base.BadRequest(response);
                else
                    return base.Ok(jsonRes);
            }
            else
                return base.Ok(jsonRes);
        }
    }
}
