using System;
using System.Collections.Generic;
using System.Data;
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
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();
        

        private static List<string> GetFileNamesToProcess()
        {
            var files = Directory.GetFiles(Settings.Default.LogFileDirectory, Settings.Default.LogFileMask, SearchOption.AllDirectories).ToList();
            Log.Info("{0} files to be processed", files.Count);

            return files;
        }

        public static void Main()
        {
            var stopwatch = Stopwatch.StartNew();
            Log.Info("Started");

            var table = PrepareBulkCopyDataTable();

            foreach (var fileName in GetFileNamesToProcess())
            {
                Log.Info("Processing file '{0}'", fileName);

                try
                {
                    IisLogFile.Parse(fileName, table);
                }
                catch (Exception ex)
                {
                    Log.Warn(ex, "Unable to parse file '{0}'. Skipping.", fileName);
                    
                    // Don't insert any of the entries in the log file since there was an error parsing
                    table.Clear();
                    IisLogFile.MarkFileAsError(fileName);

                    continue;
                }

                try
                {
                    IisLogFile.BulkCopy(table);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to insert the log entries into the database.");
                    IisLogFile.MarkFileAsError(fileName);

                    continue;
                }
                
                IisLogFile.MarkFileAsSuccess(fileName);
            }

            stopwatch.Stop();
            Log.Info("Program finished. Elapsed time: {0} seconds ({1} ms)", stopwatch.ElapsedMilliseconds / 1000, stopwatch.ElapsedMilliseconds);
        }

        private static DataTable PrepareBulkCopyDataTable()
        {
            Log.Debug("Creating DataTable");

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