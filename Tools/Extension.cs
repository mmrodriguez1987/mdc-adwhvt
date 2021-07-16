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
    }
}
