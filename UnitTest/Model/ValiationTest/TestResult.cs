using System;
using UnitTest.DAL;

namespace UnitTest.Model.ValidationTest
{

    public class TestResult
    {
        private string  _ccnValTest, _error, _description;
        private Result result;
        private ResultDetail resultDetail;
        private Int64  _testID;
        private Int16 _stateID;
        private DateTime _startDate, _endDate, _testDate;

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
        public DateTime TestDate { get => _testDate; set => _testDate = value; }

        public void recordUntitValidationTest(Int64 cdcCount, Int64 dtwhCount)
        {
            result = new Result(_ccnValTest);
            resultDetail = new ResultDetail(_ccnValTest);

            result.StateID = _stateID;
            result.TestID = _testID;
            result.Description = _description;
            result.StartDate = _startDate;
            result.EndDate = _endDate;
            result.TestDate = _testDate;
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
            result.TestDate = _testDate;
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
            result.TestDate = _testDate;
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
            result.TestDate = _testDate;
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
    }
}
