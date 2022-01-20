using System;
using System.Collections.Generic;
using System.Text;

namespace Tools.Statistics

{
    public class StatisticalEvaluation
    {
        private DateTime intialDate;
        private DateTime endDate;
        private Int32 evalDateIndex;
        private Int64 val;

        public StatisticalEvaluation()
        {

        }

        public DateTime IntialDate { 
            get => intialDate; 
            set => intialDate = value;
        }

        public DateTime EndDate { 
            get => endDate; 
            set => endDate = value; 
        }
        public int EvalDateIndex { 
            get => evalDateIndex; 
            set => evalDateIndex = value; 
        }
        public long Val {
            get => val; 
            set => val = value; 
        }
    }
}
