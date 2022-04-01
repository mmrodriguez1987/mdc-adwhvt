using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using UnitTest.DAL;

namespace UnitTest.Model.ValidationTest
{

    public class TestResult
    {
        private string _ccnValTest, _error, _description, _urlAzureTables;
        private Result result;
        private ResultDetail resultDetail;
        //private DB dataBase;
        private Int64 _testID;
        private Int16 _stateID;
        private DateTime _startDate, _endDate, _calculationDate;
        
        public enum Entity
        {
            DimAccount, DimAddress, DimDate, DimFiscalCal, DimPer,
            DimPrem, DimRate, DimSA, DimSQI, DimUOM, FactBilledUsage
        }
        
        public enum TestGenericName
        {
            New, Updated, Distinct, DistinctVsHistoric, Statistical,
            BillSegmentCountOnFact, AcountCountOnFact, FinancialTransactionCountOnFact ,
            BillOnWeekend, BillOnFiscalYear, BillOnHistoric
        }

        public enum BusinessStar 
        {
            BU, SA
        }

        public TestResult(string _ccn)
        {
            _ccnValTest = _ccn;
          
        }
        public TestResult()
        {
         
        }

        public string CcnValTest { get => _ccnValTest; set => _ccnValTest = value; }
        public string Error { get => _error; set => _error = value; }
        public string Description { get => _description; set => _description = value; }       
       
        public long TestID { get => _testID; set => _testID = value; }
        public short StateID { get => _stateID; set => _stateID = value; }        
        public DateTime StartDate { get => _startDate; set => _startDate = value; }
        public DateTime EndDate { get => _endDate; set => _endDate = value; }
        public DateTime CalcDate { get => _calculationDate; set => _calculationDate = value; }
        public DB DB { get; private set; }
        public string CcnAzureTables { get => _urlAzureTables; set => _urlAzureTables = value; }

        public void recordUntitValidationTest(Int64 cdcCount, Int64 dtwhCount)
        {
            result = new Result(_ccnValTest);
            resultDetail = new ResultDetail(_ccnValTest);

            result.StateID = _stateID;
            result.TestID = _testID;
            result.Description = _description;
            result.StartDate = _startDate;
            result.EndDate = _endDate;
            result.CalculationDate = _calculationDate;
            result.Insert();
            _error = (!String.IsNullOrEmpty(result.Error)) ? result.Error : String.Empty;

            if (result.ResultID > 0 )
            {               
                //Insertig values for CDC               
                resultDetail.ResultID = result.ResultID;
                resultDetail.ResultTypeID = 1;
                resultDetail.Count = cdcCount;
                resultDetail.Insert();
                _error = (!String.IsNullOrEmpty(resultDetail.Error)) ? resultDetail.Error : String.Empty;

                //Inserting values for DTWH
                resultDetail.ResultID = result.ResultID;
                resultDetail.ResultTypeID = 2;
                resultDetail.Count = dtwhCount;
                resultDetail.Insert();
                _error = (!String.IsNullOrEmpty(resultDetail.Error)) ? resultDetail.Error : String.Empty;
            }
        }

        public void recordHistoricalValidationTest(Int64 maxHistoricalCount)
        {
            result = new Result(_ccnValTest);
            resultDetail = new ResultDetail(_ccnValTest);

            result.StateID = _stateID;
            result.TestID = _testID;
            result.Description = _description;
            result.StartDate = _startDate;
            result.EndDate = _endDate;
            result.CalculationDate = _calculationDate;
            result.Insert();
            _error = (!String.IsNullOrEmpty(result.Error)) ? result.Error : String.Empty;

            if (result.ResultID > 0) //if exist a ResultID
            {
                //Insertig values for CDC               
                resultDetail.ResultID = result.ResultID;
                resultDetail.ResultTypeID = 4;
                resultDetail.Count = maxHistoricalCount;
                resultDetail.Insert();
                _error = (!String.IsNullOrEmpty(resultDetail.Error)) ? resultDetail.Error : String.Empty;               
            }
        }

        public void recordBilledUsageBusinessRuleValidationTest(Int64 dtwhCount, String affectedDesc, String[] affectedIDs)
        {
            result = new Result(_ccnValTest);
            resultDetail = new ResultDetail(_ccnValTest);

            result.StateID = _stateID;
            result.TestID = _testID;
            result.Description = _description;
            result.StartDate = _startDate;
            result.EndDate = _endDate;
            result.CalculationDate = _calculationDate;
            result.Insert();
            _error = (!String.IsNullOrEmpty(result.Error)) ? result.Error : String.Empty;

            if (result.ResultID > 0 && (affectedIDs != null))
            {                
                    //Insertig values for CDC               
                    resultDetail.ResultID = result.ResultID;
                    resultDetail.ResultTypeID = 2;
                    resultDetail.Count = dtwhCount;
                    resultDetail.AffectedDesc = affectedDesc;
                    resultDetail.AffectedIDs = String.Join("|", affectedIDs);
                    resultDetail.Insert();
                    _error = (!String.IsNullOrEmpty(resultDetail.Error)) ? resultDetail.Error : String.Empty;               
                
            }
        }
        
        public void recordStatisticalValidationTest(Double weekAverageCount, Int64 evaluatedCount)
        {
            result = new Result(_ccnValTest);
            resultDetail = new ResultDetail(_ccnValTest);

            result.StateID = _stateID;
            result.TestID = _testID;
            result.Description = _description;
            result.StartDate = _startDate;
            result.EndDate = _endDate;
            result.CalculationDate = _calculationDate;
            result.Insert();
            _error = (!String.IsNullOrEmpty(result.Error)) ? result.Error : String.Empty;

            if (result.ResultID > 0)
            {
                //Insertig values for Average Count               
                resultDetail.ResultID = result.ResultID;
                resultDetail.ResultTypeID = 3;
                resultDetail.Count = weekAverageCount;
                resultDetail.Insert();
                _error = (!String.IsNullOrEmpty(resultDetail.Error)) ? resultDetail.Error : String.Empty;

                //Inserting values for DTWH
                resultDetail.ResultID = result.ResultID;
                resultDetail.ResultTypeID = 2;
                resultDetail.Count = evaluatedCount;
                resultDetail.Insert();
                _error = (!String.IsNullOrEmpty(resultDetail.Error)) ? resultDetail.Error : String.Empty;
            }
        }
    
        /// <summary>
        /// This method create the Notification for the test result
        /// </summary>
        /// <param name="businessStar"></param>
        /// <param name="entity"></param>
        /// <param name="testName"></param>
        /// <param name="cdcCount"></param>
        /// <param name="dtwCount"></param>
        /// <param name="env"></param>
        /// <param name="fail"></param>
        /// <returns></returns>
        public String createMessageNotification( BusinessStar businessStar, Entity entity, TestGenericName testName, Int64 cdcCount, Int64 dtwCount, String env, Boolean fail)
        {
           String msg = String.Empty;
           switch (entity)
            {
                case Entity.DimAccount:
                    if (testName == TestGenericName.New)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        } 
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Updated)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount source and target update record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount source and target update record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Distinct)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount source and target record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount source and target record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount, Ok!";
                        }
                    }
                    if (testName == TestGenericName.DistinctVsHistoric)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                            {
                                if (dtwCount == 0 && cdcCount == 0)                                
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount data no found: DWH: " + dtwCount + ", Hist:  " + cdcCount;
                                 else                                
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;
                                
                            }                              
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount total count exceed maximun historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Statistical)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount data not found: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAccount Statistical average, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAccount Statistical average, Ok!";
                        }
                    }
                    break;               
                case Entity.DimPer:
                    if (testName == TestGenericName.New)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Updated)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer source and target updated record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer, Ok!: DTWH: " + dtwCount + ", CDC:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Distinct)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer, Ok!";
                        }
                    }
                    if (testName == TestGenericName.DistinctVsHistoric)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                            {
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer data not found: DTWH: " + dtwCount + ", Hist:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;

                            }
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer total count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Statistical)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer data not found: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPer Statistical average, ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPer Statistical average, ok!";
                        }
                    }
                    break;
                case Entity.DimUOM:
                    if (testName == TestGenericName.New)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Updated)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM source and target updated record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM, Ok!: DTWH: " + dtwCount + ", CDC:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Distinct)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM, Ok!";
                        }
                    }
                    if (testName == TestGenericName.DistinctVsHistoric)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                            {
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM data not found: DTWH: " + dtwCount + ", Hist:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;

                            }
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM total count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Statistical)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM data not found: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimUOM Statistical average, ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimUOM Statistical average, ok!";
                        }
                    }
                    break;
                case Entity.DimPrem:
                    if (testName == TestGenericName.New)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Updated)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem source and target updated record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem, Ok!: DTWH: " + dtwCount + ", CDC:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Distinct)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem, Ok!";
                        }
                    }
                    if (testName == TestGenericName.DistinctVsHistoric)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                            {
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem data not found: DTWH: " + dtwCount + ", Hist:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;

                            }
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem total count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Statistical)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem data not found: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimPrem Statistical average, ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem Statistical average, ok!";
                        }
                    }
                    break;
                case Entity.DimSQI:
                    if (testName == TestGenericName.New)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSQI source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSQI, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Updated)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI source and target updated record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimPrem, Ok!: DTWH: " + dtwCount + ", CDC:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSQI, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Distinct)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSQI source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSQI, Ok!";
                        }
                    }
                    if (testName == TestGenericName.DistinctVsHistoric)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                            {
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI data not found: DTWH: " + dtwCount + ", Hist:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;

                            }
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSQI total count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSQI, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Statistical)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI data not found: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSQI Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSQI Statistical average, ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSQI Statistical average, ok!";
                        }
                    }
                    break;
                case Entity.DimAddress:
                    if (testName == TestGenericName.New)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Updated)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress source and target updated record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress, Ok!: DTWH: " + dtwCount + ", CDC:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Distinct)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress, Ok!";
                        }
                    }
                    if (testName == TestGenericName.DistinctVsHistoric)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                            {
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress data not found: DTWH: " + dtwCount + ", Hist:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;

                            }
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress total count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Statistical)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress data not found: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimAddress Statistical average, ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimAddress Statistical average, ok!";
                        }
                    }
                    break;
                case Entity.DimSA:
                    if (testName == TestGenericName.New)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA source and target new record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Updated)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA source and target updated record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA, Ok!: DTWH: " + dtwCount + ", CDC:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Distinct)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA source and target total record count do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA, Ok!";
                        }
                    }
                    if (testName == TestGenericName.DistinctVsHistoric)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                            {
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA data not found: DTWH: " + dtwCount + ", Hist:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;

                            }
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA total count exceed maximum historic: DWH: " + dtwCount + ", Hist:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA, Ok!";
                        }
                    }
                    if (testName == TestGenericName.Statistical)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                if (dtwCount == 0 && cdcCount == 0)
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA data not found: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                                else
                                    msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA Statistical Average is out of 10-day range: 10-day AVG: " + dtwCount + ", Evaluated:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "DimSA Statistical average, ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "DimSA Statistical average, ok!";
                        }
                    }
                    break;
                case Entity.FactBilledUsage:
                    if (testName == TestGenericName.BillSegmentCountOnFact)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "BillSegment count ccb and BilledUsage do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "BillSegment count ccb and BilledUsage do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "BillSegment on BilledUsage, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "BillSegment on BilledUsage, Ok!";
                        }
                    }
                    if (testName == TestGenericName.AcountCountOnFact)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "Account count ccb and BilledUsage do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "Account count ccb and BilledUsage do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "Account on BilledUsage, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "Account on BilledUsage, Ok!";
                        }
                    }
                    if (testName == TestGenericName.FinancialTransactionCountOnFact)
                    {
                        if (fail)
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "Financial Transacction count ccb and BilledUsage do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "Financial Transacction count ccb and BilledUsage do not match: DWH: " + dtwCount + ", CCB:  " + cdcCount;
                        }
                        else
                        {
                            if (businessStar == BusinessStar.BU)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-BU: " : "ADF-BU: ") + "Financial Transacction on BilledUsage, Ok!";
                            if (businessStar == BusinessStar.SA)
                                msg = ((env == "DEV" || env == "STA") ? "ADF-DEV-SA: " : "ADF-SA: ") + "Financial Transacction on BilledUsage, Ok!";
                        }
                    }

                    break;
           }
            return msg;
        }
        
        public async Task recordDataValidationAzureStorageAsync(string JSONResult)
        {          

            var json = JsonConvert.SerializeObject(JSONResult);
            var data = new StringContent(json, Encoding.UTF8, "application/json");
            var url = "https://prod-63.eastus2.logic.azure.com:443/workflows/041784ca92844421b84a0d170fce42a0/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=Bt5NXXiZSZ5Eof9Mm1X_3KQddA-0_AgXjaWI8q9Uvao";
            using var client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            var response = await client.PostAsync(url, data);
            var result = await response.Content.ReadAsStringAsync();
            Console.WriteLine(result);
  
        }
        
        public string getTestResultJSONFormat(DataSet ds)
        {
            TransformedResult result;
            List<TransformedResult> listResult = new List<TransformedResult>();

            foreach(DataRow dr in ds.Tables[0].Rows)
            {
                result = new TransformedResult();
                result.testID = Convert.ToInt64(dr["testID"]);
                result.stateID = Convert.ToInt64(dr["stateID"]);
                result.Description = dr["description"].ToString();
                result.StartDate = Convert.ToDateTime(dr["startDate"]);
                result.EndDate = Convert.ToDateTime(dr["endDate"]);
                result.CCBCount = Convert.ToDouble(dr["CCBCount"]);
                result.DWHCount = Convert.ToDouble(dr["DWHCount"]);
                result.CCBAver = Convert.ToDouble(dr["CCBAver"]);
                result.CCBMax = Convert.ToDouble(dr["CCBMax"]);
                result.calculationDate = Convert.ToDateTime(dr["calcDate"]);
                listResult.Add(result);
            }
            return JsonConvert.SerializeObject(listResult);
        }

        public Task<String> recordValidationOnAzureStorage(string JSONResult)       
        {
            return Task.Run(() =>
            {
                try
                {
                    var json = JsonConvert.SerializeObject(JSONResult);
                    var data = new StringContent(json, Encoding.UTF8, "application/json");
                    data.Headers.ContentType.CharSet = null;
                                      
                    using var client = new HttpClient();
                    
                    var response = client.PostAsync(_urlAzureTables, data);
                    
                    if (response.Result.IsSuccessStatusCode)
                    
                        return "OK";
                    else 
                        return response.Result.RequestMessage.ToString();      
                }
                catch (Exception ex)
                {
                    return ex.Message.ToString();
                }
            });
                   
        }
    
    }
}
