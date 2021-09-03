using System;
using System.Collections.Generic;
using System.Text;

namespace Tools.Mechanism
{
    public class DDictionary
    {
        public Dictionary<string, string> messages = new Dictionary<string, string>();
        
        
        public DDictionary()
        {
            messages.Add("UT.BeginTest", "************* UNIT TEST BEGIN ************");
            messages.Add("UT.CCBConxStr", "CCB CONEXION STRING ==>> ");
            messages.Add("UT.DTWHConxStr", "DTWH CONEXION STRING ==>> ");
            messages.Add("UT.CDCProdCnx", "CDC Prod CONEXION STRING ==>> ");
            messages.Add("UT.CDCStgeCnx", "CDC Stge CONEXION STRING ==>> ");

            messages.Add("UT.CCBConxStrErr", "Error on Oracle(CCB) conexion: ");
            messages.Add("UT.DTWHConxStrErr", "Error on Datawarehpouse conexion: ");

            messages.Add("UT.EndTest", "************* UNIT TEST END **************");
            messages.Add("UT.BadEnvOrIdent", "Error: Enviroment or table Identifier can't be NULL or EMPTY");
            messages.Add("UT.BadEnv", "Invalid enviroment");
            messages.Add("UT.BadIdent", "Invalid tableIdentifier");
            messages.Add("UT.BadEndDate", "endDate can not be major to the current day");
            messages.Add("UT.BadStartDate", "startDate can not be major that endDate");
        }

    }
}
