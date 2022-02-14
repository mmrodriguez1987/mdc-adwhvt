using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Xml.Serialization;

namespace Tools.DataConversion
{
    /// <summary>
    /// Class Extensions
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Convierte el contenido de un dataset en una variable <see cref="System.String"/> que contiene la respuesta en <see cref="System.Xml"/>
        /// </summary>
        /// <param name="ds"><see cref="System.Data.DataSet"/> con los datos a convertir</param>
        /// <returns><see cref="System.String"/> que contiene la respuesta en <see cref="System.Xml"/></returns>
        public static string ToXml(this DataSet ds)
        {
            using (var memoryStream = new MemoryStream())
            {
                using (TextWriter streamWriter = new StreamWriter(memoryStream))
                {
                    var xmlSerializer = new XmlSerializer(typeof(DataSet));
                    xmlSerializer.Serialize(streamWriter, ds);
                    return Encoding.UTF8.GetString(memoryStream.ToArray());
                }
            }
        }

        /// <summary>
        /// Convert a DataTabla into Json
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static string DataTableToJSONWithStringBuilder(DataTable table)
        {
            var JSONString = new StringBuilder();
            if (table.Rows.Count > 0)
            {
                //JSONString.Append("[");
                for (int i = 0; i < table.Rows.Count; i++)
                {
                    JSONString.Append("{");
                    for (int j = 0; j < table.Columns.Count; j++)
                    {
                        if (j < table.Columns.Count - 1)
                        {
                            JSONString.Append("\"" + table.Columns[j].ColumnName.ToString() + "\":" + "\"" + table.Rows[i][j].ToString() + "\",");
                        }
                        else if (j == table.Columns.Count - 1)
                        {
                            JSONString.Append("\"" + table.Columns[j].ColumnName.ToString() + "\":" + "\"" + table.Rows[i][j].ToString() + "\"");
                        }
                    }
                    if (i == table.Rows.Count - 1)
                    {
                        JSONString.Append("}");
                    }
                    else
                    {
                        JSONString.Append("},");
                    }
                }
                //JSONString.Append("]");
            }
            return JSONString.ToString();
        }

        public static string messageToJSON(string msg)
        {         
            DataTable dt = new DataTable();   
            dt.Columns.Add("msg");    
            dt.Rows.Add(msg);
            return DataTableToJSONWithStringBuilder(dt);
        }


        /// <summary>
        /// Get delimited string fom a DataTable
        /// </summary>
        /// <param name="table">data</param>
        /// <param name="filter">filter to apply</param>
        /// <param name="ID"></param>
        /// <param name="delimiter"></param>
        /// <returns>Delimited String with the response</returns>
        public static string GetDelimitedString(DataTable table, String filter, String ID, String delimiter)
        {
            
            DataRow[] rows = table.Select(filter);
            string[] args = new string[rows.Length];
            

            for (int i = 0; i < rows.Length; i++)                            
                args[i] = rows[i][ID].ToString();
            

            return String.Join(delimiter, args);
        }


        /// <summary>
        /// Prepara the response structure on Dataset
        /// </summary>
        /// <returns>a Dataset with a structure ready to be converted in JSON with Details about the test result
        ///  | State | Test Information | Entitites Envolved | Test Result Description | Start Date | End Date | Count CDC | Count DTW | query CDC | query DTW | Effect Date | SMS test |
        /// </returns>
        public static DataSet getResponseStructure(String dataTableName)
        {
            DataSet dsResult = new DataSet("dsResults");
            DataTable TestResult = new DataTable(dataTableName);

            TestResult.Columns.Add("State");
            TestResult.Columns.Add("Test Information");
            TestResult.Columns.Add("Entities Involved");
            TestResult.Columns.Add("Test Result Description");
            TestResult.Columns.Add("Initial Evaluated Date");
            TestResult.Columns.Add("End Evaluated Date");
            TestResult.Columns.Add("count_CDC");
            TestResult.Columns.Add("count_DTW");
            TestResult.Columns.Add("query_CDC");
            TestResult.Columns.Add("query_DTW");
            TestResult.Columns.Add("Effectuated Date");
            TestResult.Columns.Add("SMS Text");
            TestResult.Rows.Add(0, 0, "", "", "", "", 0, 0,"","","","");

            dsResult.Tables.Add(TestResult);
            return dsResult;
        }

        public static DataSet getResponseEnviromentInfoStructure(String dataTableName)
        {
            DataSet dsResult = new DataSet("dsResults");
            DataTable rs = new DataTable(dataTableName);

            rs.Columns.Add("State");
            rs.Columns.Add("Variable");
            rs.Columns.Add("Description");
            rs.Columns.Add("Value"); 
            dsResult.Tables.Add(rs);
            return dsResult;
        }
    }
}
