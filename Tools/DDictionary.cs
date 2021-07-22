using System;
using System.Collections.Generic;
using System.Text;

namespace Tools.Mechanism
{
    public class DDictionary
    {
        public Dictionary<string, string> utMsgs = new Dictionary<string, string>();
        
        
        public DDictionary()
        {
            utMsgs.Add("UTC.BeginTest", "************* UNIT TEST BEGIN ************");
            utMsgs.Add("UTC.CCBConxStr", "CCB CONEXION STRING ==>> ");
            utMsgs.Add("UTC.DTWHConxStr", "DTWH CONEXION STRING ==>> ");

            utMsgs.Add("UTC.CCBConxStrErr", "Error on Oracle(CCB) conexion: ");
            utMsgs.Add("UTC.DTWHConxStrErr", "Error on Datawarehpouse conexion: ");

            utMsgs.Add("UTC.EndTest", "************* UNIT TEST END **************");
            utMsgs.Add("UTC.BadEnvOrIdent", "Error: Enviroment or table Identifier can't be NULL or EMPTY");
            utMsgs.Add("UTC.BadEnv", "Error: Enviroment has an invalid value ");
            utMsgs.Add("UTC.BadIdent", "Error: tableIdentifier has an invalid value ");
        }

    }
}
