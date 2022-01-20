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
            for (int i=0;  i < DaysToEvaluate; i++)
            {
                //Si No Incluye fines de semana                
                if (!IncludeWeekend)
                {
                    // Si el Dia es diferente de Sabado Y Diferente de Domingo
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
            }

            return evalRange;
        }
    }
}
