using System;
using System.Collections.Generic;
using System.Text;

namespace Tools
{
    public class CBDate
    {
        List<DateTime> evalRange;
        public CBDate()
        {
            evalRange = new List<DateTime>();
        }

        public List<DateTime> GetEvalRangeDate(DateTime evalDate, Int32 DaysToEvaluate, Boolean IncludeWeekend)
        {         
            DateTime DayOfRangeDate = evalDate;
            int i = 0;
          
            
            do
            {
                //Si No Incluye fines de semana                
                if (!IncludeWeekend)
                {
                    // Si el dia es diferente de sabado y domingo
                    if (DayOfRangeDate.DayOfWeek != DayOfWeek.Saturday && DayOfRangeDate.DayOfWeek != DayOfWeek.Sunday)
                    {
                        evalRange.Add(DayOfRangeDate);
                        i++;
                    }
                }
                else
                {
                    evalRange.Add(DayOfRangeDate);
                    i++;
                }
                //Pasar al dia anterior
                DayOfRangeDate = DayOfRangeDate.AddDays(-1);
            } while (i < DaysToEvaluate);

            return evalRange;
        }
    }
}
