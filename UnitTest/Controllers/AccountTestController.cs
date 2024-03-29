﻿using Microsoft.AspNetCore.Mvc;
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
    public class AccountTestController : ControllerBase
    {       
     
        private readonly ILogger<AccountTestController> _log;       
        private DataSet finalResultDS, dsResult;
        private string testFileName;
        private SMS mySMS;
        private Global gbl;

        public AccountTestController(IConfiguration conf, ILogger<AccountTestController> log)
        {
            gbl = new Global(conf);            
            _log = log;      
            finalResultDS = new DataSet();
            dsResult = new DataSet();
            testFileName = @gbl.LogFileRoot + "UT_BI_ADWH_" + DateTime.UtcNow.ToString("yyyy_MM_dd");
        }
             
        
        [HttpGet]
        public async Task<IActionResult> Get(DateTime startDate, DateTime endDate)
        {
            //Local members
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");

            //Adding 10am to the hour
            startDate = startDate.Date.AddHours(10);
            endDate = endDate.Date.AddHours(10);

            //Initializing the communication service
            mySMS = new SMS(gbl.CcnAzureCommunicationServices);

            //Initializing Account Business Layer
            Account acct = new Account(gbl.CcnDatawareHouse, gbl.CcnCDC, gbl.CcnValidationDB, gbl.CcnAzureCommunicationServices, testFileName);         

            
            //Validation: Get Distinct Accounts Count between the CCB and DTW
            dsResult = await acct.DistinctAccountCount(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            

            //Validation: Get Distinct Account On load Over the Maximun Historic Count
            dsResult = await acct.DistinctAcctCountOnDataLoadOverTheMaxHistricCount(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            
            //Validation: Get comparision of new records between CCB and DWT
            dsResult = await acct.NewAccountCounts(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());


            //Validation: Get comparision of updated records between CCB and DTW
            dsResult = await acct.UpdatedAccountCounts(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());


            //Validation: Get a 5 Days statistical comparision of account
            dsResult = await acct.StatisticalAcountEvaluation(endDate, gbl.EvaluatedDatesRangeOnAverageTest, gbl.ToleranceVariatonNumber);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
        }
    }
}
