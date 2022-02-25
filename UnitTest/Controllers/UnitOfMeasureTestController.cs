﻿using Microsoft.AspNetCore.Mvc;
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
    public class UnitOfMeasureTestController : ControllerBase
    {    
        private DataSet finalResultDS, dsResult;       
        private SMS mySMS;
        private Global gbl;

        public UnitOfMeasureTestController(IConfiguration conf)
        {
            gbl = new Global(conf);              
            finalResultDS = new DataSet();
            dsResult = new DataSet();           
        }

        [HttpGet]
        public async Task<IActionResult> Get(DateTime startDate, DateTime endDate, Boolean sendSMSNotify, Boolean saveResult)
        {
            #region Controller Initialization   
            //Local members
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");           
            startDate = startDate.Date.AddHours(10);
            endDate = endDate.Date.AddHours(10);            
            mySMS = new SMS(gbl.CcnACS);
            UnitOfMeasure uom = new UnitOfMeasure(gbl.CcnDTW, gbl.CcnCDC, gbl.CcnDVT);
            dsResult = Extensions.getResponseStructure("");
            finalResultDS = Extensions.getResponseStructure("");
            #endregion

            #region Validation: Persons Count     
            dsResult = await uom.UOMCount(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify )
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            if (dsResult.Tables[0].Rows[0][11].ToString().StartsWith("Error"))            
                return base.BadRequest(dsResult.Tables[0].Rows[0][11].ToString());            
            else
            {
                if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            }
            #endregion

            #region Validation New Person
            dsResult = await uom.NewUOMCount(startDate,endDate, saveResult);
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

            #region Validation: Updated Person
            dsResult = await uom.UpdatedUOMCount(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify )
                mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());

            if (dsResult.Tables[0].Rows[0][11].ToString().StartsWith("Error"))
                return base.BadRequest(dsResult.Tables[0].Rows[0][11].ToString());
            else
            {
                if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            }
            #endregion

            #region Validation: Statistical comparision
            dsResult = await uom.StatisticalUOMEvaluation(endDate, gbl.EvaluatedDatesRangeOnAverageTest, gbl.ToleranceVariatonNumber, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);

            if (dsResult.Tables[0].Rows[0][11].ToString().StartsWith("Error"))            
                return base.BadRequest(dsResult.Tables[0].Rows[0][11].ToString());            
            else
            {
                if ((dsResult.Tables[0].Rows[0][0].ToString() == "Warning" || dsResult.Tables[0].Rows[0][0].ToString() == "Failed") && sendSMSNotify)
                    mySMS.SendSMS(gbl.FromPhNumbAlert, gbl.BiTeamPhoneNumbers, dsResult.Tables[0].Rows[0][11].ToString());
            }
            #endregion

            #region Validation: Person Count vs Max Historic
            dsResult = await uom.UOMCountVsMaxHistoric(startDate, endDate, saveResult);
            finalResultDS.Tables[0].ImportRow(dsResult.Tables[0].Rows[0]);
            if (dsResult.Tables[0].Rows[0][11].ToString().StartsWith("Error"))            
                return base.BadRequest(dsResult.Tables[0].Rows[0][11].ToString());
            
            #endregion
            
            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(finalResultDS.Tables[0]));
        }
    }
}