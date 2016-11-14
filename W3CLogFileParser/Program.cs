using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using NLog;
using W3CLogFileParser.Properties;

namespace W3CLogFileParser
{
    public static class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        private const char FieldDelimiter = ' ';

        public static void Main(string[] args)
        {
            var stopwatch = Stopwatch.StartNew();
            Logger.Info("===============");
            Logger.Info("Program started");

            var files = Directory.GetFiles(Settings.Default.LogFileDirectory, Settings.Default.LogFileMask, SearchOption.AllDirectories).ToList();
            Logger.Info($"{files.Count} files to be processed");

            Logger.Debug("Creating DataTable");
            var table = PrepareBulkCopyDataTable();

            foreach (var file in files)
            {
                Logger.Info($"Processing file {file}");

                string[] lines;

                try
                {
                    lines = File.ReadAllLines(file);
                }
                catch (Exception ex)
                {
                    Logger.Warn(ex, $"Unable to read file {file}. Skipping.");
                    continue;
                }

                var fileHasErrors = false;
                string[] fieldOrder = null;

                foreach (var line in lines)
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

                    try
                    {
                        var entry = new IisRequestLogEntry(fieldOrder, FieldDelimiter, line);
                        entry.AddToDataTable(table);
                    }
                    catch (Exception ex)
                    {
                        fileHasErrors = true;
                        Logger.Error(ex, $"An error occured trying to parse the line [{line}] and add it to the DataTable. Skipping file.");
                        break;
                    }
                }

                if (!fileHasErrors)
                {
                    try
                    {
                        Logger.Debug($"Bulk inserting {table.Rows.Count} rows...");

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
                                DestinationTableName = Settings.Default.DestinationTable
                            };

                            connection.Open();
                            bulkCopy.WriteToServer(table);
                            connection.Close();
                        }

                        Logger.Debug("Finished bulk insert.");
                    }
                    catch (SqlException ex)
                    {
                        Logger.Error(ex, "Failed to insert the records into the database.");
                        fileHasErrors = true;
                    }
                }

                table.Clear();

                if (fileHasErrors)
                {
                    Logger.Warn($"File {file} encountered errors during parsing. The records in the file were not inserted into the database.");

                    var renameTo = file + ".wrn";
                    Logger.Debug($"Renaming file to {renameTo}");
                    File.Move(file, renameTo);
                }
                else
                {
                    var renameTo = file + ".scc";
                    Logger.Debug($"Renaming file to {renameTo}");
                    File.Move(file, renameTo);
                }
            }

            stopwatch.Stop();
            Logger.Info($"Program finished. Elapsed time: {stopwatch.ElapsedMilliseconds / 1000} seconds ({stopwatch.ElapsedMilliseconds} ms)");
        }

        private static DataTable PrepareBulkCopyDataTable()
        {
            var table = new DataTable("IisRequest");
            table.Columns.Add(new DataColumn("Id", typeof(long)));
            table.Columns.Add(new DataColumn("InsertDate", typeof(SqlDateTime)));
            table.Columns.Add(new DataColumn("Date", typeof(SqlDateTime)));
            table.Columns.Add(new DataColumn("Time", typeof(SqlString)));
            table.Columns.Add(new DataColumn("ServerName", typeof(SqlString)));
            table.Columns.Add(new DataColumn("ServerIP", typeof(SqlBinary)));
            table.Columns.Add(new DataColumn("ServerPort", typeof(int)));
            table.Columns.Add(new DataColumn("Method", typeof(SqlString)));
            table.Columns.Add(new DataColumn("UriStem", typeof(SqlString)));
            table.Columns.Add(new DataColumn("UriQuery", typeof(SqlString)));
            table.Columns.Add(new DataColumn("TimeTakenInMilliseconds", typeof(int)));
            table.Columns.Add(new DataColumn("ProtocolStatus", typeof(short)));
            table.Columns.Add(new DataColumn("ProtocolSubstatus", typeof(short)));
            table.Columns.Add(new DataColumn("ProtocolVersion", typeof(SqlString)));
            table.Columns.Add(new DataColumn("ClientIP", typeof(SqlBinary)));
            table.Columns.Add(new DataColumn("UserAgent", typeof(SqlString)));
            table.Columns.Add(new DataColumn("Referrer", typeof(SqlString)));
            table.Columns.Add(new DataColumn("Username", typeof(SqlString)));
            table.Columns.Add(new DataColumn("Cookie", typeof(SqlString)));
            table.Columns.Add(new DataColumn("Win32Status", typeof(long)));
            table.Columns.Add(new DataColumn("ServiceName", typeof(SqlString)));
            table.Columns.Add(new DataColumn("Host", typeof(SqlString)));
            table.Columns.Add(new DataColumn("BytesSent", typeof(long)));
            table.Columns.Add(new DataColumn("BytesReceived", typeof(long)));

            return table;
        }
    }
}