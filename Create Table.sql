create schema Logs;
go

if object_id('Logs.IisRequest') is not null
    drop table Logs.IisRequest;
go

create table Logs.IisRequest
(
    Id bigint identity(0, 1) primary key not null,
    InsertDate smalldatetime not null,
    [Date] smalldatetime, -- SqlBulkCopy doesn't support "Date"
    [Time] char(8), -- SqlBulkCopy doesn't support "Time"
    ServerName varchar(63),
    ServerIP varbinary(16),
    ServerPort int,
    Method varchar(10),
    UriStem varchar(8000),
    UriQuery varchar(8000),
    TimeTakenInMilliseconds int,
    ProtocolStatus smallint,
    ProtocolSubstatus smallint,
    ProtocolVersion varchar(10),
    ClientIP varbinary(16),
    UserAgent varchar(1500),
    Referrer varchar(8000),
    Username varchar(1000),
    Cookie varchar(1000),
    Win32Status bigint,
    ServiceName varchar(100),
    Host varchar(1000),
    BytesSent bigint,
    BytesReceived bigint
);
go

if object_id('Logs.IPv4ToBinary') is not null
    drop function Logs.IPv4ToBinary;
go

create function Logs.IPv4ToBinary(@ip as varchar(15)) returns table
as return(
    select cast(cast(cast(parsename(@ip, 4) as integer) as binary(1)) +
                cast(cast(parsename(@ip, 3) as integer) as binary(1)) +
                cast(cast(parsename(@ip, 2) as integer) as binary(1)) +
                cast(cast(parsename(@ip, 1) as integer) as binary(1))
           as varbinary(16)) as BinaryIPv4
);
go

-- To watch the row count increase as the bulk copy is running
select count(*) from Logs.IisRequest with (nolock)
