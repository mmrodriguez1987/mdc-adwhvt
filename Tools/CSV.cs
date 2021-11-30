using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace Tools.Files
{
    public class CSV
    {
        private String _path;
       

        public string Path { get => _path; set => _path = value; }

        public CSV(String path)
        {
            _path = path;
        }

        /// <summary>
        /// Write the Data Log into a Exsisting or New file.
        /// </summary>
        /// <param name="dataTable">Information to write</param>
        public void writeNewOrExistingFile (DataTable dataTable)
        {
            string path = Directory.GetCurrentDirectory();

            if (!File.Exists(_path)) 
            {
                var config = new CsvConfiguration(CultureInfo.CurrentCulture)
                {
                    // Don't write the header again.
                    HasHeaderRecord = false,
                };

                using (var stream = File.Open(_path, FileMode.Append))
                using (var writer = new StreamWriter(stream))
                using (var csv = new CsvWriter(writer, config))
                {
                    foreach (DataColumn column in dataTable.Columns)
                    {
                        csv.WriteField(column.ColumnName);
                    }

                    csv.NextRecord();

                    foreach (DataRow row in dataTable.Rows)
                    {
                        for (var i = 0; i < dataTable.Columns.Count; i++)
                        {
                            csv.WriteField(row[i]);
                        }
                        csv.NextRecord();
                    }

                }

            } 
            else
            {
                // Write to a file.
                using (var writer = new StreamWriter(_path))
                using (var csv = new CsvWriter(writer, CultureInfo.CurrentCulture))
                {
                    foreach (DataRow row in dataTable.Rows)
                    {
                        for (var i = 0; i < dataTable.Columns.Count; i++)
                        {
                            csv.WriteField(row[i]);
                        }
                        csv.NextRecord();
                    }
                }

            }





        }
            
    }
}
