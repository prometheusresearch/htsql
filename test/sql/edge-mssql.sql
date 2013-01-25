--
-- Copyright (c) 2006-2013, Prometheus Research, LLC
--


-- The `edge` database contains testing data for some edge cases.


-- Data Types and Values.

CREATE SCHEMA [type];


-- In MS SQL, type BIT is used to store Boolean values.  BIT is a numeric type
-- with two values: 1 and 0.

CREATE TABLE [type].[boolean] (
    [code]          VARCHAR(32) PRIMARY KEY,
    [bit]           BIT
);

INSERT INTO [type].[boolean] ([code], [bit]) VALUES
    ('1-true', 'TRUE');
INSERT INTO [type].[boolean] ([code], [bit]) VALUES
    ('2-false', 'FALSE');
INSERT INTO [type].[boolean] ([code], [bit]) VALUES
    ('3-null', NULL);


-- MS SQL supports 1-, 2-, 4-, and 8-byte integers.

CREATE TABLE [type].[integer] (
    [code]          VARCHAR(32) PRIMARY KEY,
    [tinyint]       TINYINT,
    [smallint]      SMALLINT,
    [int]           INT,
    [bigint]        BIGINT
);

INSERT INTO [type].[integer] ([code], [tinyint], [smallint], [int], [bigint]) VALUES
    ('1-regular', 60, 3600, 12960000, 167961600000000);
INSERT INTO [type].[integer] ([code], [tinyint], [smallint], [int], [bigint]) VALUES
    ('2-min', 0, -32768, -2147483648, -9223372036854775808);
INSERT INTO [type].[integer] ([code], [tinyint], [smallint], [int], [bigint]) VALUES
    ('3-max', 255, 32767, 2147483647, 9223372036854775807);
INSERT INTO [type].[integer] ([code], [tinyint], [smallint], [int], [bigint]) VALUES
    ('4-null', NULL, NULL, NULL, NULL);


-- Inexact floating numbers.

CREATE TABLE [type].[float] (
    [code]          VARCHAR(32) PRIMARY KEY,
    [real]          REAL,
    [float]         FLOAT
);

INSERT INTO [type].[float] ("code", [real], [float]) VALUES
    ('1-regular', 271828e-5, 27182818285e-10);
INSERT INTO [type].[float] ("code", [real], [float]) VALUES
    ('2-small', 1.175494351e-38, 2.2250738585072014e-308);
INSERT INTO [type].[float] ("code", [real], [float]) VALUES
    ('3-large', 3.402823466e+38, 1.7976931348623157e+308);
INSERT INTO [type].[float] ("code", [real], [float]) VALUES
    ('4-null', NULL, NULL);


-- Exact decimal numbers.

CREATE TABLE [type].[decimal] (
    [code]          VARCHAR(32) PRIMARY KEY,
    [decimal]       DECIMAL,
    [decimal_6]     DECIMAL(6),
    [decimal_6_2]   DECIMAL(6,2),
    [numeric]       NUMERIC
);

INSERT INTO [type].[decimal] ([code], [decimal], [decimal_6], [decimal_6_2], [numeric]) VALUES
    ('1-regular', 479001600, 362880, 3543.75, 479001600);
INSERT INTO [type].[decimal] ([code], [decimal], [decimal_6], [decimal_6_2], [numeric]) VALUES
    ('2-max', 999999999999999999, 999999, 9999.99, 999999999999999999);
INSERT INTO [type].[decimal] ([code], [decimal], [decimal_6], [decimal_6_2], [numeric]) VALUES
    ('3-null', NULL, NULL, NULL, NULL);


-- String data types.

CREATE TABLE [type].[string] (
    [code]          VARCHAR(32) PRIMARY KEY,
    [char]          CHAR,
    [char_8]        CHAR(8),
    [nchar]         NCHAR,
    [nchar_8]       NCHAR(8),
    [varchar]       VARCHAR,
    [varchar_8]     VARCHAR(8),
    [varchar_max]   VARCHAR(MAX),
    [nvarchar]      NVARCHAR,
    [nvarchar_8]    NVARCHAR(8),
    [nvarchar_max]  NVARCHAR(MAX)
);

INSERT INTO [type].[string] ([code], [char], [char_8], [nchar], [nchar_8], [varchar], [varchar_8], [varchar_max], [nvarchar], [nvarchar_8], [nvarchar_max]) VALUES
    ('1-regular', 'H', 'HTSQL', 'H', 'HTSQL', 'H', 'HTSQL', 'HTSQL', 'H', 'HTSQL', 'HTSQL');
INSERT INTO [type].[string] ([code], [char], [char_8], [nchar], [nchar_8], [varchar], [varchar_8], [varchar_max], [nvarchar], [nvarchar_8], [nvarchar_max]) VALUES
    ('2-unicode', N'λ', N'λόγος', N'λ', N'λόγος', N'λ', N'λόγος', N'λόγος', N'λ', N'λόγος', N'λόγος');
INSERT INTO [type].[string] ([code], [char], [char_8], [nchar], [nchar_8], [varchar], [varchar_8], [varchar_max], [nvarchar], [nvarchar_8], [nvarchar_max]) VALUES
    ('3-empty', '', '', '', '', '', '', '', '', '', '');
INSERT INTO [type].[string] ([code], [char], [char_8], [nchar], [nchar_8], [varchar], [varchar_8], [varchar_max], [nvarchar], [nvarchar_8], [nvarchar_max]) VALUES
    ('4-null', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


-- MS SQL does not have an Enum data type.

CREATE TABLE [type].[enum] (
    [code]          VARCHAR(32) PRIMARY KEY
);


-- DateTime data types.
-- FIXME: MSSQL 2008 adds DATETIME2 type.

CREATE TABLE [type].[datetime] (
    [code]          VARCHAR(32) PRIMARY KEY,
    [smalldatetime] SMALLDATETIME,
    [datetime]      DATETIME
);

INSERT INTO [type].[datetime] ([code], [smalldatetime], [datetime]) VALUES
    ('1-regular', '2010-04-15 20:13', '2010-04-15 20:13:04.5');
INSERT INTO [type].[datetime] ([code], [smalldatetime], [datetime]) VALUES
    ('2-min', '1900-01-01 00:00', '1753-01-01 00:00:00.000');
INSERT INTO [type].[datetime] ([code], [smalldatetime], [datetime]) VALUES
    ('3-max', '2079-06-06 23:59', '9999-12-31 23:59:59.998');
INSERT INTO [type].[datetime] ([code], [smalldatetime], [datetime]) VALUES
    ('4-null', NULL, NULL);

-- MS SQL has no native DATE type (FIXME: MS SQL 2008 adds DATE type).

CREATE TABLE [type].[date] (
    [code]          VARCHAR(32) PRIMARY KEY
);


-- MS SQL has no native TIME type (FIXME: MS SQL 2008 adds TIME type).

CREATE TABLE [type].[time] (
    [code]          VARCHAR(32) PRIMARY KEY
);


-- Unsupported data types.

CREATE TABLE [type].[other] (
    [code]          VARCHAR(32) PRIMARY KEY,
    [money]         MONEY,
    [binary_8]      BINARY(8),
    [uniqueidentifier] UNIQUEIDENTIFIER,
    [xml]           XML
);

INSERT INTO [type].[other] ([code], [money], [binary_8], [uniqueidentifier], [xml]) VALUES
    ('1-regular', 1000.00, 0x00FF00FF00FF00FF, '6F9619FF-8B86-D011-B42D-00C04FC964FF', '<title>HTSQL</title>');
INSERT INTO [type].[other] ([code], [money], [binary_8], [uniqueidentifier], [xml]) VALUES
    ('2-null', NULL, NULL, NULL, NULL);


-- Entity Names.

CREATE SCHEMA [name];

CREATE TABLE [name].[Три Поросенка] (
    ["Ниф-Ниф"]     NVARCHAR(16),
    [`Нуф-Нуф`]     NVARCHAR(16),
    [[Наф-Наф]]]    NVARCHAR(16)
);

INSERT INTO [name].[Три Поросенка] (["Ниф-Ниф"], [`Нуф-Нуф`], [[Наф-Наф]]]) VALUES
    ('соломенный', 'деревянный', 'каменный');


