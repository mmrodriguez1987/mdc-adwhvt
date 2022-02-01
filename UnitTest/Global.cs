using Microsoft.Extensions.Configuration;
using System;

namespace UnitTest
{
    

    public class Global
    {    
        private string[] _BITEAM;
        private DateTime _startDate, _endDate;
        private int _DTVAL_BU_MAX_COUNT_DISTINCT_BILL_IDs;      
        private int _DTVAL_BU_MAX_COUNT_DISTINCT_ACCT_IDs;
        private int _EvaluatedDatesRangeOnOrdinaryTest;
        private int _EvaluatedDatesRangeOnAverageTest;
        private string _dataWarehouseCCN, _validTestDB, _CdcCCN, _AzureCCN, _fromSMS, _defaulFileRoot;        
        private Double _toleranceVariationNumber;


        public string[] BiTeamPhoneNumbers { get => _BITEAM; set => _BITEAM = value; }
        public string CcnDatawareHouse { get => _dataWarehouseCCN; set => _dataWarehouseCCN = value; }
        public string CcnCDC { get => _CdcCCN; set => _CdcCCN = value; }
        public string CcnAzureCommunicSrvs { get => _AzureCCN; set => _AzureCCN = value; }
        public string FromPhNumbAlert { get => _fromSMS; set => _fromSMS = value; }
        public string LogFileRoot{ get => _defaulFileRoot; set => _defaulFileRoot = value; }
        
        public DateTime StartDate { get => _startDate; set => _startDate = value; }
        public DateTime EndDate { get => _endDate; set => _endDate = value; }
        public int DTVAL_BU_MAX_COUNT_DISTINCT_BILL_IDs { get => _DTVAL_BU_MAX_COUNT_DISTINCT_BILL_IDs; set => _DTVAL_BU_MAX_COUNT_DISTINCT_BILL_IDs = value; }
        public int DTVAL_BU_MAX_COUNT_DISTINCT_ACCT_IDs { get => _DTVAL_BU_MAX_COUNT_DISTINCT_ACCT_IDs; set => _DTVAL_BU_MAX_COUNT_DISTINCT_ACCT_IDs = value; }
        
        /// <summary>
        /// Incremental/Decremental Tolerance Number on average of account
        /// </summary>
        public double ToleranceVariatonNumber { get => _toleranceVariationNumber; set => _toleranceVariationNumber = value; }
        public string CcnValidationDB { get => _validTestDB; set => _validTestDB = value; }       
        public int EvaluatedDatesRangeOnAverageTest { get => _EvaluatedDatesRangeOnAverageTest; set => _EvaluatedDatesRangeOnAverageTest = value; }
        public int EvaluatedDateRangeOnOrdinaryTest { get => _EvaluatedDatesRangeOnOrdinaryTest; set => _EvaluatedDatesRangeOnOrdinaryTest = value; }

        public Global(IConfiguration conf)
        {
            //conexions
            _BITEAM = new string[] {
                conf.GetValue<string>("PrjVar:TO_DEV1_PHONE_NUMBER").ToString(),
                //conf.GetValue<string>("PrjVar:TO_DEV2_PHONE_NUMBER").ToString(),
                //conf.GetValue<string>("PrjVar:TO_DEV3_PHONE_NUMBER").ToString()
             };
            _dataWarehouseCCN = conf.GetConnectionString("DTWttdpConnection");
            _CdcCCN = conf.GetConnectionString("CDCProdConnection");
            _AzureCCN = conf.GetConnectionString("azure_sms_ccn");
            _validTestDB = conf.GetConnectionString("ValidDBConnection");
            _fromSMS = conf.GetValue<string>("PrjVar:FROM_PHONE_NUMBER");
            _defaulFileRoot = conf.GetValue<string>("PrjVar:FILE_ROOT");
            
            _toleranceVariationNumber = conf.GetValue<double>("PrjVar:TOLE_VAR_NUMBER");
            _DTVAL_BU_MAX_COUNT_DISTINCT_BILL_IDs = conf.GetValue<int>("PrjVar:DTVAL_BU_MAX_COUNT_DISTINCT_BILL_IDs");
            _DTVAL_BU_MAX_COUNT_DISTINCT_ACCT_IDs = conf.GetValue<int>("PrjVar:DTVAL_BU_MAX_COUNT_DISTINCT_ACCT_IDs");
            _EvaluatedDatesRangeOnAverageTest = conf.GetValue<int>("PrjVar:EVAL_DATES_RANGE_ON_AVE_TEST");
            _EvaluatedDatesRangeOnOrdinaryTest = conf.GetValue<int>("PrjVar:EVAL_DATES_RANGE_ON_ORD_TEST");
            _endDate = DateTime.UtcNow;
            _startDate = _endDate.AddDays(-_EvaluatedDatesRangeOnOrdinaryTest);
        }        
    }
}
