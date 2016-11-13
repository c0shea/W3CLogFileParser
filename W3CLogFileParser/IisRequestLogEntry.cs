using System;
using System.Data.Linq;
using System.Net;

namespace W3CLogFileParser
{
    public class IisRequestLogEntry
    {
        public DateTime? Date { get; set; }
        public TimeSpan? Time { get; set; }
        public string ServerName { get; set; }
        public IPAddress ServerIP { get; set; }
        public int? ServerPort { get; set; }
        public string Method { get; set; }
        public string UriStem { get; set; }
        public string UriQuery { get; set; }
        public short? TimeTakenInMilliseconds { get; set; }
        public short? ProtocolStatus { get; set; }
        public short? ProtocolSubstatus { get; set; }
        public string ProtocolVersion { get; set; }
        public IPAddress ClientIP { get; set; }
        public string UserAgent { get; set; }
        public string Referrer { get; set; }
        public string Username { get; set; }
        public string Cookie { get; set; }
        public short? Win32Status { get; set; }
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
                        UriStem = val;
                        break;
                    case "cs-uri-query":
                        UriQuery = val;
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
                        UserAgent = val;
                        break;
                    case "cs(Cookie)":
                        Cookie = val;
                        break;
                    case "cs(Referer)":
                        Referrer = val;
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
                        Win32Status = short.Parse(val);
                        break;
                    case "sc-bytes":
                        BytesSent = long.Parse(val);
                        break;
                    case "cs-bytes":
                        BytesReceived = long.Parse(val);
                        break;
                    case "time-taken":
                        TimeTakenInMilliseconds = short.Parse(val);
                        break;
                }
            }
        }

        public void SaveToDatabase()
        {
            using (var db = new DataWarehouseDataContext())
            {
                db.IisRequests.InsertOnSubmit(new IisRequest
                {
                    InsertDate = DateTime.Now,
                    Date = Date,
                    Time = Time,
                    ServerName = ServerName,
                    ServerIP = ServerIP == null ? null : new Binary(ServerIP.GetAddressBytes()),
                    ServerPort = ServerPort,
                    Method = Method,
                    UriStem = UriStem,
                    UriQuery = UriQuery,
                    TimeTakenInMilliseconds = TimeTakenInMilliseconds,
                    ProtocolStatus = ProtocolStatus,
                    ProtocolSubstatus = ProtocolSubstatus,
                    ProtocolVersion = ProtocolVersion,
                    ClientIP = ClientIP == null ? null : new Binary(ClientIP.GetAddressBytes()),
                    UserAgent = UserAgent,
                    Referrer = Referrer,
                    Username = Username,
                    Win32Status = Win32Status,
                    ServiceName = ServiceName,
                    Host = Host,
                    BytesSent = BytesSent,
                    BytesReceived = BytesReceived
                });

                db.SubmitChanges();
            }
        }
    }
}