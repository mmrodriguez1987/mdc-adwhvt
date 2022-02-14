using Microsoft.Extensions.Configuration;
using System;

namespace UnitTest
{   
    public class Global
    {
        #region Private members
        private string[] _phoneArrayBITEAM;        
        private int _evaluatedDatesRangeOnOrdinaryTest, _evaluatedDatesRangeOnAverageTest;       
        private string _dataWarehouseCCN, _valTestCCN, _ccbCCN, _cdcCCN, _azComServCCN, _fromSMS;        
        private Double _toleranceVariationNumber;
        #endregion

        #region Connection Strings        
        public string CcnDTW { get => _dataWarehouseCCN; set => _dataWarehouseCCN = value; }
        public string CcnCDC { get => _cdcCCN; set => _cdcCCN = value; }
        public string CcnCCB { get => _ccbCCN; set => _ccbCCN = value; }
        public string CcnACS { get => _azComServCCN; set => _azComServCCN = value; }
        public string CcnDVT { get => _valTestCCN; set => _valTestCCN = value; }
        #endregion

        #region Global Constants
        public string[] BiTeamPhoneNumbers { get => _phoneArrayBITEAM; set => _phoneArrayBITEAM = value; }
        public string FromPhNumbAlert { get => _fromSMS; set => _fromSMS = value; }      
       
        public double ToleranceVariatonNumber { get => _toleranceVariationNumber; set => _toleranceVariationNumber = value; }        
        public int EvaluatedDatesRangeOnAverageTest { get => _evaluatedDatesRangeOnAverageTest; set => _evaluatedDatesRangeOnAverageTest = value; }
        public int EvaluatedDateRangeOnOrdinaryTest { get => _evaluatedDatesRangeOnOrdinaryTest; set => _evaluatedDatesRangeOnOrdinaryTest = value; }
        #endregion

        public Global(IConfiguration conf)
        {            
            _phoneArrayBITEAM = new string[] {
                conf.GetValue<string>("ValidationTest:TO_DEV1_PHONE_NUMBER").ToString(),
                conf.GetValue<string>("ValidationTest:TO_DEV2_PHONE_NUMBER").ToString(),
                conf.GetValue<string>("ValidationTest:TO_DEV3_PHONE_NUMBER").ToString()
             };
            _dataWarehouseCCN = conf.GetConnectionString("DTWConnection");
            _cdcCCN = conf.GetConnectionString("CDCConnection");
            _azComServCCN = conf.GetConnectionString("ACSConnection");
            _valTestCCN = conf.GetConnectionString("DVTConnection");
            _ccbCCN = conf.GetConnectionString("CCBConnection");
            _fromSMS = conf.GetValue<string>("ValidationTest:FROM_PHONE_NUMBER");                      
            _toleranceVariationNumber = conf.GetValue<double>("ValidationTest:TOLE_VAR_NUMBER");
            _evaluatedDatesRangeOnAverageTest = conf.GetValue<int>("ValidationTest:EVAL_DATES_RANGE_ON_AVE_TEST");
            _evaluatedDatesRangeOnOrdinaryTest = conf.GetValue<int>("ValidationTest:EVAL_DATES_RANGE_ON_ORD_TEST");                  
        }        
    }
}
