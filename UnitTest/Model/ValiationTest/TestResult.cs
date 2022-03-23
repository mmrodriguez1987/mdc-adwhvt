using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using UnitTest.DAL;
using UnitTest.Model.ValiationTest;

namespace UnitTest.Model.ValidationTest
{

    public class TestResult
    {
        private string _ccnValTest, _error, _description;
        private Result result;
        private ResultDetail resultDetail;
        private DB dataBase;
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

        public string CcnValTest { get => _ccnValTest; set => _ccnValTest = value; }
        public string Error { get => _error; set => _error = value; }
        public string Description { get => _description; set => _description = value; }       
       
        public long TestID { get => _testID; set => _testID = value; }
        public short StateID { get => _stateID; set => _stateID = value; }        
        public DateTime StartDate { get => _startDate; set => _startDate = value; }
        public DateTime EndDate { get => _endDate; set => _endDate = value; }
        public DateTime CalcDate { get => _calculationDate; set => _calculationDate = value; }
        public DB DB { get; private set; }

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
        
       
        
        public Task<String> getTestResultJSONFormat(DateTime evalDate)
        {
            return Task.Run(() =>
            {
                try
                {
                    DataSet dsTestResult = new DataSet();
                    dataBase = new DB(_ccnValTest);

                    dsTestResult = dataBase.GetObjecFromViewtDS("vwTestResult", "CAST(calculationDate AS DATE) = '" + evalDate.Date + "'", "resultID Desc", "*");

                    List<TransformedResult> resultCollection = new List<TransformedResult>();
                    

                    foreach (DataRow row in dsTestResult.Tables[0].Rows)
                    {
                        TransformedResult trsResult = new TransformedResult();

                        trsResult.resultID = Convert.ToInt64(row["resultID"]);
                        trsResult.Test = row["Test"].ToString();
                        trsResult.Type = row["Type"].ToString();
                        trsResult.Description = row["Description"].ToString();
                        trsResult.StartDate = Convert.ToDateTime(row["Start Date"]);
                        trsResult.EndDate = Convert.ToDateTime(row["End Date"]);
                        trsResult.calculationDate = Convert.ToDateTime(row["calculationDate"]);
                        trsResult.DWHCount = DBNull.Value.Equals(row["DWH Count"]) ? 0 : Convert.ToDouble(row["DWH Count"]);
                        trsResult.CCBCount = DBNull.Value.Equals(row["CCB Count"]) ? 0 : Convert.ToDouble(row["CCB Count"]);
                        trsResult.CCBAver = DBNull.Value.Equals(row["CCB Aver Count"]) ? 0 : Convert.ToDouble(row["CCB Aver Count"]);
                        trsResult.CCBMax = DBNull.Value.Equals(row["CCB Max Hist Count"]) ? 0 : Convert.ToDouble(row["CCB Max Hist Count"]);
                        resultCollection.Add(trsResult);      
                    }
              

                    string json = JsonConvert.SerializeObject(resultCollection, Formatting.Indented, new JsonSerializerSettings()
                    {
                        ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                    });

                    Console.WriteLine(json);
                    // {
                    //   "Table1": [
                    //     {
                    //       "id": 0,
                    //       "item": "item 0"
                    //     },
                    //     {
                    //       "id": 1,
                    //       "item": "item 1"
                    //     }
                    //   ]
                    // }                  

                    return json;
                }
                catch (Exception e)
                {
                    return e.ToString();
                }
            });
        }    
    }
}
