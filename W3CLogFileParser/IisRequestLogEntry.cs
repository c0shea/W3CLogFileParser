using System;
using System.Data;
using System.Net;

namespace W3CLogFileParser
{
    public class IisRequestLogEntry
    {
        private const int MaxStringLength = 8000;

        public DateTime? Date { get; set; }
        public TimeSpan? Time { get; set; }
        public string ServerName { get; set; }
        public IPAddress ServerIP { get; set; }
        public int? ServerPort { get; set; }
        public string Method { get; set; }
        public string UriStem { get; set; }
        public string UriQuery { get; set; }
        public int? TimeTakenInMilliseconds { get; set; }
        public short? ProtocolStatus { get; set; }
        public short? ProtocolSubstatus { get; set; }
        public string ProtocolVersion { get; set; }
        public IPAddress ClientIP { get; set; }
        public string UserAgent { get; set; }
        public string Referrer { get; set; }
        public string Username { get; set; }
        public string Cookie { get; set; }
        public long? Win32Status { get; set; }
        public string ServiceName { get; set; }
        public string Host { get; set; }
        public long? BytesSent { get; set; }
        public long? BytesReceived { get; set; }

        public IisRequestLogEntry(string[] fieldOrder, char delimiter, string line)
        {
            var fields = line.Split(delimiter);

            if (fieldOrder.Length != fields.Length)
            {
                throw new ArgumentException("Line doesn't contain enough fields to match the field order");
            }

            for (int i = 0; i < fieldOrder.Length; i++)
            {
                var val = fields[i];

                // Field doesn't have a value
                if (val == "-")
                {
                    continue;
                }

                switch (fieldOrder[i])
                {
                    case "date":
                        Date = DateTime.Parse(val);
                        break;
                    case "time":
                        Time = TimeSpan.Parse(val);
                        break;
                    case "s-sitename":
                        ServiceName = val;
                        break;
                    case "s-computername":
                        ServerName = val;
                        break;
                    case "s-ip":
                        ServerIP = IPAddress.Parse(val);
                        break;
                    case "cs-method":
                        Method = val;
                        break;
                    case "cs-uri-stem":
                        UriStem = val.Truncate(MaxStringLength);
                        break;
                    case "cs-uri-query":
                        UriQuery = val.Truncate(MaxStringLength);
                        break;
                    case "s-port":
                        ServerPort = int.Parse(val);
                        break;
                    case "cs-username":
                        Username = val;
                        break;
                    case "c-ip":
                        ClientIP = IPAddress.Parse(val);
                        break;
                    case "cs-version":
                        ProtocolVersion = val;
                        break;
                    case "cs(User-Agent)":
                        UserAgent = val.Truncate(1500);
                        break;
                    case "cs(Cookie)":
                        Cookie = val;
                        break;
                    case "cs(Referer)":
                        Referrer = val.Truncate(MaxStringLength);
                        break;
                    case "cs-host":
                        Host = val;
                        break;
                    case "sc-status":
                        ProtocolStatus = short.Parse(val);
                        break;
                    case "sc-substatus":
                        ProtocolSubstatus = short.Parse(val);
                        break;
                    case "sc-win32-status":
                        Win32Status = long.Parse(val);
                        break;
                    case "sc-bytes":
                        BytesSent = long.Parse(val);
                        break;
                    case "cs-bytes":
                        BytesReceived = long.Parse(val);
                        break;
                    case "time-taken":
                        TimeTakenInMilliseconds = int.Parse(val);
                        break;
                }
            }
        }

        public void AddToDataTable(DataTable table)
        {
            table.Rows.Add(
                1, // Id
                DateTime.Now, // InsertDate
                Date,
                Time.HasValue ? Time.ToString() : (object)DBNull.Value,
                ServerName,
                ServerIP.GetAddressBytes(),
                ServerPort,
                Method,
                UriStem,
                UriQuery,
                TimeTakenInMilliseconds,
                ProtocolStatus,
                ProtocolSubstatus,
                ProtocolVersion,
                ClientIP.GetAddressBytes(),
                UserAgent,
                Referrer,
                Username,
                Cookie,
                Win32Status,
                ServiceName,
                Host,
                BytesSent,
                BytesReceived
            );
        }
    }
}
