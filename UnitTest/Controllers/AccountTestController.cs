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
    [Author("Michael Rodriguez", "02/11/2022", "Controller for Test Incremental Load at Azure Data Factory")]
    public class AccountTestController : ControllerBase
    {  
        private DataSet finalResultDS, dsResult;
       
        private SMS mySMS;
        private Global gbl;
        private TestResult ts;

        public AccountTestController(IConfiguration conf)
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
            #region Controller Intitialization
            //Initializing members
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");            
            startDate = startDate.Date.AddHours((startHour== 0) ? gbl.StartHour : startHour);
            endDate = endDate.Date.AddHours((endHour==0) ? gbl.EndHour : endHour);            
            mySMS = new SMS(gbl.CcnACS);            
            Account acct = new Account(gbl.CcnDTW, gbl.CcnCDC, gbl.CcnDVT);
            #endregion

            #region Validation: Accounts Count
            dsResult = await acct.AccountCount(startDate, endDate);
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
            /*
            #region Validation: Account Count vs Historic
            dsResult = await acct.TotalAcctCountVsMaxHist(startDate, endDate);
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
            #region Validation: New Records
            dsResult = await acct.NewAccountCount(startDate, endDate);
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
            
             #region Validation: Updated account
             dsResult = await acct.UpdatedAccountCounts(startDate, endDate);
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

             #region Validation: Statistical Comparision
            
             dsResult = await acct.StatisticalAcountEvaluation(endDate, gbl.EvaluatedDatesRangeOnAverageTest, gbl.ToleranceVariatonNumber);
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
