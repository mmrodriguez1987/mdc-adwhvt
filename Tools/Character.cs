using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;

namespace Tools
{ /// <summary>
  /// Class Character.
  /// </summary>
    public class Character
    {
        /// <summary>
        /// Remueve acentos diacriticos de una cadena
        /// </summary>
        /// <param name="stIn">Cadena que se desea transformar</param>
        /// <returns>Cadena de texto transformada</returns>
        public static String RemoverDiacriticos(string stIn)
        {
            string con = "áàäéèëíìïóòöúùuñÁÀÄÉÈËÍÌÏÓÒÖÚÙÜçÇ";
            string sin = "aaaeeeiiiooouuunAAAEEEIIIOOOUUUcC";
            for (int i = 0; i < con.Length; i++)
            {
                stIn = stIn.Replace(con[i], sin[i]);
            }
            return stIn;
        }

        /// <summary>
        /// Remueve los espacios demas en una cadena de texto segun lo indiciado
        /// </summary>
        /// <param name="str">Cadena de texto que se desea transformar</param>
        /// <param name="all">'True' si todos los espacios, 'False' si solo los dobles espacios</param> 
        /// <returns>Cadena de texto transformada</returns>
        public static String RemoverEspacios(string str, Boolean all)
        {
            if (all)
            {
                for (int i = 0; i < str.Length; i++)
                {
                    str = str.Replace(" ", "");
                }
            }
            else
            {
                for (int i = 0; i < str.Length; i++)
                {
                    str = str.Replace("  ", " ");
                }
            }

            return str.Trim();
        }

        /// <summary>
        /// Convierte una cadena <see cref="System.String"/> a tipo oracion
        /// </summary>
        /// <param name="str"><see cref="System.String"/> que se desea convertir</param>
        /// <returns><see cref="System.String"/> convertido</returns>
        public static String TitleCase(string str)
        {
            if (String.IsNullOrEmpty(str))
                return String.Empty;
            else
                return new CultureInfo("en-US", false).TextInfo.ToTitleCase(str.ToLower());
        }

        /// <summary>
        /// Codifica los numeros de telefonos contenidos en un Datatable como emails en una cadena
        /// de texto para ser enviados mediante envio masivo de correo electronico
        /// </summary>
        /// <param name="dt">DataTable de origen</param>
        /// <param name="Columna">Indice de la columna</param>
        /// <param name="delimitador">Caracter delimitador</param>
        /// <param name="gateway">Dominiam gateway</param>
        /// <returns>Retorna un String con el resultado de la operacion</returns>
        public static String EncodeEmails(DataTable dt, Int32 Columna, String delimitador, String gateway)
        {
            string st = String.Empty;
            for (int i = 0; i <= dt.Rows.Count - 1; i++)
            {
                if (i == (dt.Rows.Count - 1))
                {
                    st += dt.Rows[i][Columna].ToString() + gateway;
                }
                else
                {
                    st += dt.Rows[i][Columna].ToString() + gateway + delimitador;
                }
            }
            return st;
        }
    }

   
}
