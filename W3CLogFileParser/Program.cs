using System;
using System.IO;
using System.Linq;
using NLog;
using W3CLogFileParser.Properties;

namespace W3CLogFileParser
{
    public class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        private const char FieldDelimiter = ' ';

        public static void Main(string[] args)
        {
            Logger.Info("Program started");

            var files = Directory.GetFiles(Settings.Default.LogFileDirectory, Settings.Default.LogFileMask, SearchOption.AllDirectories).ToList();

            Logger.Info($"{files.Count} files to be processed");

            foreach (var file in files)
            {
                Logger.Info($"Processing file {file}");

                var lines = File.ReadAllLines(file);

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
                        entry.SaveToDatabase();
                    }
                    catch (Exception ex)
                    {
                        fileHasErrors = true;
                        Logger.Error(ex, "An error occured trying to parse and save the line in the database.");
                    }
                }

                if (fileHasErrors)
                {
                    Logger.Warn($"File {file} encountered errors during parsing. Some records may have been skipped.");

                    var renameTo = file + ".wrn";
                    Logger.Info($"Renaming file from {file} to {renameTo}");
                    File.Move(file, renameTo);
                }
                else
                {
                    var renameTo = file + ".scc";
                    Logger.Info($"Renaming file from {file} to {renameTo}");
                    File.Move(file, renameTo);
                }
            }

            Logger.Info("Program finished");
        }
    }
}