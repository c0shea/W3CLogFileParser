using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using NLog;
using W3CLogFileParser.Properties;

namespace W3CLogFileParser
{
    public static class IisLogFile
    {
        private const char FieldDelimiter = ' ';
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();

        public static void Parse(string fileName, DataTable table)
        {
            using (var fileStream = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.Write))
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8))
            {
                string[] fieldOrder = null;
                string line;

                while ((line = streamReader.ReadLine()) != null)
                {
                    if (line.StartsWith("#Fields:"))
                    {
                        fieldOrder = line.Replace("#Fields: ", "").Split(FieldDelimiter);
                        continue;
                    }

                    if (line.StartsWith("#"))
                    {
                        // Skip comment lines
                        continue;
                    }

                    if (fieldOrder == null)
                    {
                        throw new InvalidOperationException("Field order has not been defined in the log file");
                    }

                    var entry = new IisRequestLogEntry(fieldOrder, FieldDelimiter, line);
                    entry.AddToDataTable(table);
                }
            }
        }

        public static void BulkCopy(DataTable table)
        {
            Log.Debug("Bulk inserting {0} rows...", table.Rows.Count);

            using (SqlConnection connection = new SqlConnection(Settings.Default.DataWarehouseConnectionString))
            {
                var bulkCopy = new SqlBulkCopy
                (
                    connection,
                    SqlBulkCopyOptions.TableLock |
                    SqlBulkCopyOptions.FireTriggers |
                    SqlBulkCopyOptions.UseInternalTransaction,
                    null
                )
                {
                    DestinationTableName = Settings.Default.DestinationTable,
                    BatchSize = 5_000,
                    BulkCopyTimeout = 30 // Seconds
                };

                connection.Open();
                bulkCopy.WriteToServer(table);
                connection.Close();
            }

            table.Clear();

            Log.Debug("Finished bulk insert");
        }

        public static void MarkFileAsError(string fileName)
        {
            Log.Warn("File '{0}' encountered errors during parsing. The records in the file were not inserted into the database.", fileName);

            var renameTo = $"{fileName}.wrn";
            Log.Debug("Renaming file to '{0}'", renameTo);

            File.Move(fileName, renameTo);
        }

        public static void MarkFileAsSuccess(string fileName)
        {
            var renameTo = $"{fileName}.scc";
            Log.Debug("Renaming file to '{0}'", renameTo);

            File.Move(fileName, renameTo);
        }
    }
}
