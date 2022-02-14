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
    public class PersonTestController : ControllerBase
    {    
        private DataSet finalResultDS, dsResult;       
        private SMS mySMS;
        private Global gbl;

        public PersonTestController(IConfiguration conf)
        {
            gbl = new Global(conf);              
            finalResultDS = new DataSet();
            dsResult = new DataSet();
           
        }

        [HttpGet]
        public async Task<IActionResult> Get(DateTime startDate, DateTime endDate, Boolean sendSMSNotify)
        {
            mySMS = new SMS(gbl.CcnACS);

            Person per = new Person(gbl.CcnDTW, gbl.CcnCDC);

            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("PremiseTestResult");
          
            //Validation: Unique Persons Count on both sides
            dsResult = await per.UniquePersonsCount(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify )
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            //Validation: New Persons Count on both sides
            dsResult = await per.NewPersonsCount(startDate,endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify )
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            //Validation: Updated Persons Count on both sides
            dsResult = await per.UpdatedPersonsCount(startDate, endDate);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify )
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());           

            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
        }
    }
}
