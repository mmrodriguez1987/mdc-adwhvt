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
        public async Task<IActionResult> Get()
        {
           
            mySMS = new SMS(gbl.CCN_ACS);            
            Account acct = new Account(gbl.CCN_DTWH, gbl.CCN_CDC, testFileName);
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("GlobalTestResult");
          
            //Validation: Get Distinct Accounts Count between the CCB and DTW
            dsResult = await acct.DistinctAccountCounts(gbl.startDate, gbl.endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")            
                mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM, dsResult.Tables[0].Rows[0][11].ToString());
            
           

            //Validation: Get Distinct Account On load Over the Maximun Historic Count
            dsResult = await acct.DistinctAcctCountOnDataLoadOverTheMaxHistricCount(gbl.startDate, gbl.endDate, gbl.DTVAL_BU_MAX_COUNT_DISTINCT_ACCT_IDs);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM, dsResult.Tables[0].Rows[0][11].ToString());



            //Validation: Get comparision of new records between CCB and DWT
            dsResult = await acct.NewAccountCounts(gbl.startDate, gbl.endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM, dsResult.Tables[0].Rows[0][11].ToString());



            //Validation: Get comparision of updated records between CCB and DTW
            dsResult = await acct.UpdatedAccountCounts(gbl.startDate, gbl.endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed")
                mySMS.SendSMS(gbl.PH_FROM, gbl.PH_BITEAM, dsResult.Tables[0].Rows[0][11].ToString());

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
        }
    }
}
