using System;

namespace UnitTest.Model.ValiationTest
{
    public class TransformedResult
    {
        private Int64 _resultID;
        private string _state, _test, _type, _description;
        private DateTime _startDate, _endDate, _calculationDate;
        private double _dwhCount, _ccbCount, _ccbAver, _ccbMax;
        public Int64 resultID { get => _resultID; set=> _resultID = value; }
        public string? State { get => _state; set => _state = value; }
        public string? Test { get => _test; set => _test = value; }
        public string? Type { get => _type; set => _type = value; }
        public string? Description { get => _description; set => _description = value; }
        public DateTime StartDate { get => _startDate; set => _startDate = value; }
        public DateTime EndDate { get => _endDate; set => _endDate = value; }
        public DateTime calculationDate { get => _calculationDate; set => _calculationDate = value; }
        public Double DWHCount { get => _dwhCount; set => _dwhCount = value; }
        public Double CCBCount { get => _ccbCount; set => _ccbCount = value; }
        public Double CCBAver { get => _ccbAver; set => _ccbAver = value; }
        public Double CCBMax { get => _ccbMax; set => _ccbMax = value; }

        public TransformedResult()
        {

        }

    }
}
