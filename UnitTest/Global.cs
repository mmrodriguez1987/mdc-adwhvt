using Microsoft.Extensions.Configuration;
using System;

namespace UnitTest
{
    

    public class Global
    {    
        private string[] _BITEAM;
        private DateTime _startDate, _endDate;
        private int _evaluatedDatesRange;
        private string _dataWarehouseCCN, _CdcCCN, _AzureCCN, _fromSMS, _defaulFileRoot ;
        public string[] PH_BITEAM { get => _BITEAM; set => _BITEAM = value; }
        public string CCN_DTWH { get => _dataWarehouseCCN; set => _dataWarehouseCCN = value; }
        public string CCN_CDC { get => _CdcCCN; set => _CdcCCN = value; }
        public string CCN_ACS { get => _AzureCCN; set => _AzureCCN = value; }
        public string PH_FROM { get => _fromSMS; set => _fromSMS = value; }
        public string LogFileRoot{ get => _defaulFileRoot; set => _defaulFileRoot = value; }
        public int EvaluatedDatesRange { get => _evaluatedDatesRange; set => _evaluatedDatesRange = value; }
        public DateTime startDate { get => _startDate; set => _startDate = value; }
        public DateTime endDate { get => _endDate; set => _endDate = value; }

        public Global(IConfiguration conf)
        {
            _BITEAM = new string[] {
                conf.GetValue<string>("PrjVar:TO_DEV1_PHONE_NUMBER").ToString(),
                conf.GetValue<string>("PrjVar:TO_DEV2_PHONE_NUMBER").ToString(),
                conf.GetValue<string>("PrjVar:TO_DEV3_PHONE_NUMBER").ToString()
             };
            _dataWarehouseCCN = conf.GetConnectionString("DTWttdpConnection");
            _CdcCCN = conf.GetConnectionString("CDCProdConnection");
            _AzureCCN = conf.GetConnectionString("azure_sms_ccn");
            _fromSMS = conf.GetValue<string>("PrjVar:FROM_PHONE_NUMBER");
            _defaulFileRoot = conf.GetValue<string>("PrjVar:FILE_ROOT");
            _evaluatedDatesRange = conf.GetValue<int>("PrjVar:QTY_DAYS_TO_EVALUATE");
            _endDate = DateTime.UtcNow;
            _startDate = _endDate.AddDays(-_evaluatedDatesRange);
        }        
    }
}
