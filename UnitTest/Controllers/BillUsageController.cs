using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Threading.Tasks;
using Tools.Mechanism;
using Tools.DataConversion;
using UnitTest.Model.DataWarehouse;

namespace UnitTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BillUsageController : ControllerBase
    {
        private IConfiguration _conf { get; }
       
        private static DDictionary myDict;
        private readonly ILogger<BillUsageController> _log;

        public BillUsageController(IConfiguration conf, ILogger<BillUsageController> log)
        {
            _conf = conf;
            _log = log;
            myDict = new DDictionary();
            //billusage = new BillUsage()
        }

       private async Task<IActionResult> Get(Int64 testID, DateTime startDate, DateTime endDate)
        {
            DataSet dsResult = new DataSet();
            BillUsage bu = new BillUsage(_conf.GetConnectionString("DTWttdpConnection"));
            dsResult = bu.getResponseStructure("");



            //Validating the dates            
            if (DateTime.Compare(endDate, DateTime.Now) > 0)
            {
                _log.LogError(myDict.messages["UT.BadEndDate"]);
                return base.BadRequest(Extensions.messageToJSON(myDict.messages["UT.BadEndDate"]));
            }

            //Validating the dates            
            if (DateTime.Compare(startDate, endDate) > 0)
            {
                _log.LogError(myDict.messages["UT.BadStartDate"]);
                return base.BadRequest(Extensions.messageToJSON(myDict.messages["UT.BadStartDate"]));
            }

            dsResult = await bu.GetBillGeneratedOnWrongFiscalYear(startDate, endDate);
            return base.Ok(Extensions.DataTableToJSONWithStringBuilder(dsResult.Tables[0]));


        }


    }
}
