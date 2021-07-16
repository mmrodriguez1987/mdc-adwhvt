using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Threading.Tasks;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UnitTestController : ControllerBase
    {
        public IConfiguration _conf { get; }
        public Util objUtil;
        public UnitTestController(IConfiguration configuration)
        {
            _conf = configuration;
        }


        /// <summary>
        /// Datawarehouse Controller 
        /// </summary>
        /// <param name="tableIdentifier"></param> 
        /// <returns></returns>       
        [HttpGet]
        public async Task<IActionResult> Get(string tableIdentifier)
        {
            string dtwTable, ccbTable;

            switch (tableIdentifier.Trim())
            {
                case "UOM":
                    dtwTable = "dwadm2.CD_UOM";
                    ccbTable = "CISADM.CI_UOM";
                    break;
                case "FISCAL_CAL": case "CAL_PERIOD":
                    dtwTable = "dwadm2.CD_FISCAL_CAL";
                    ccbTable = "CISADM.CI_CAL_PERIOD";
                    break;

                case "ACCT":
                    dtwTable = "dwadm2.CD_ACCT";
                    ccbTable = "CISADM.CI_ACCT";
                    break;

                case "ADDR":
                    dtwTable = "dwadm2.CD_PREM";
                    ccbTable = "CISADM.CI_ADDR";
                    break;

                case "PREM":
                    dtwTable = "dwadm2.CD_PREM";
                    ccbTable = "CISADM.CI_PREM";
                    break;

                case "RATE": case "RS":
                    dtwTable = "dwadm2.CD_RATE";
                    ccbTable = "CISADM.CI_RS";
                    break;

                case "SA":
                    dtwTable = "dwadm2.CD_SA";
                    ccbTable = "CISADM.CI_SA";
                    break;

                case "PER": case "PERSON":
                    dtwTable = "dwadm2.CD_PER";
                    ccbTable = "CISADM.CI_PER";
                    break;

                case "SQI":
                    dtwTable = "dwadm2.CD_SQI";
                    ccbTable = "CISADM.CI_SQI";
                    break;

                default:
                    dtwTable = "";
                    ccbTable = "";
                    break;
            }


            if (String.IsNullOrEmpty(dtwTable) || String.IsNullOrEmpty(ccbTable))
                return NotFound("Datawarehouse or CCB Tables are messing");           

            new Util(_conf.GetConnectionString("CCBProdConnection"), 
                _conf.GetConnectionString("DTWttdpConnection"),
                dtwTable,ccbTable);

            DataSet dsResult = new DataSet("dsResults");

            dsResult = Util.setDSDefaultStructure(ccbTable, dtwTable);          

            //Executing count by Thread
            DataSet resultCCB = await Util.GetRowsCountFromCCB();
            DataSet resultDTWH = await Util.GetRowsCountFromDTWH();

            //if the response from Datawarehouse contain an Error
            if (resultDTWH.Tables[0].Rows[0].ItemArray[0].ToString().StartsWith("-"))
                return Ok(Util.DataTableToJSONWithStringBuilder(resultDTWH.Tables[0]));
            
            //if the response from CCB  contain an Error
            if (resultCCB.Tables[0].Rows[0].ItemArray[0].ToString().StartsWith("-"))
                return Ok(Util.DataTableToJSONWithStringBuilder(resultCCB.Tables[0]));
            else
            {
                dsResult.Tables[0].Rows[0][0] = resultDTWH.Tables[0].Rows[0].ItemArray[0];
                dsResult.Tables[0].Rows[0][1] = resultCCB.Tables[0].Rows[0].ItemArray[0];

                if (Convert.ToInt64(dsResult.Tables[0].Rows[0][0]) == Convert.ToInt64(dsResult.Tables[0].Rows[0][1]))
                    dsResult.Tables[0].Rows[0][2] = 1;

                return Ok(Util.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));
            }
                     
        }
    }
}
