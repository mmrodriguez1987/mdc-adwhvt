using System;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Text;

namespace Tools
{
    /// <summary> 
    /// Clase que encapsula todos aquellos metodos que cumplen funciones de ejecucion en el Simbolo del Sistema.      
    /// </summary>    
    public class CommandPrompt
    {
        /// <summary>
        /// Metodo que hace un Ping al Servidor de Produccion
        /// </summary>
        /// <param name="server">IP del Servidor/PC que se desea realizar el PING</param>
        /// <returns>Retorna <see cref="System.Boolean"/> conteniendo el estado del PING; True ==> Eco, False ==> No hubo eco</returns>     
        public static bool Ping(string server)
        {
            Ping pn = new Ping();
            PingReply pr;
            IPAddress ip = IPAddress.Parse(server);
            pr = pn.Send(ip);
            if (pr.Status == IPStatus.Success)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        /// <summary>
        /// Ejecutar cualquier comando en el dos
        /// </summary>
        /// <param name="_Command">String que contiene las sentencia a ejectuar</param>
        public static void ExecuteCommand(string _Command)
        {
            //Indicamos que deseamos inicializar el proceso cmd.exe junto a un comando de arranque. 
            //(/C, le indicamos al proceso cmd que deseamos que cuando termine la tarea asignada se cierre el proceso).
            //Para mas informacion consulte la ayuda de la consola con cmd.exe /? 
            System.Diagnostics.ProcessStartInfo procStartInfo = new System.Diagnostics.ProcessStartInfo("cmd", "/c " + _Command);
            // Indicamos que la salida del proceso se redireccione en un Stream
            procStartInfo.RedirectStandardOutput = true;
            procStartInfo.UseShellExecute = false;
            //Indica que el proceso no despliegue una pantalla negra (El proceso se ejecuta en background)
            procStartInfo.CreateNoWindow = false;
            //Inicializa el proceso
            System.Diagnostics.Process proc = new System.Diagnostics.Process();
            proc.StartInfo = procStartInfo;
            proc.Start();
            //Consigue la salida de la Consola(Stream) y devuelve una cadena de texto
            string result = proc.StandardOutput.ReadToEnd();
            //Muestra en pantalla la salida del Comando
            Console.WriteLine(result);
        }
    }
}
