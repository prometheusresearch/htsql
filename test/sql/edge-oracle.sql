--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- See `LICENSE` for license information, `AUTHORS` for the list of authors.
--


-- The `edge` database contains testing data for some edge cases.


-- Data Types and Values.


-- Oracle has no native Boolean type, so Boolean is emulated as
-- `NUMBER(1)` with a check constraint `IN (0, 1)`.

CREATE TABLE "boolean" (
    "code"          VARCHAR2(32) PRIMARY KEY,
    "number_1"      NUMBER(1) CHECK ("number_1" IN (0, 1))
);

INSERT INTO "boolean" ("code", "number_1") VALUES
    ('1-true', 1);
INSERT INTO "boolean" ("code", "number_1") VALUES
    ('2-false', 0);
INSERT INTO "boolean" ("code", "number_1") VALUES
    ('3-null', NULL);


-- Oracle has no native binary integer type; HTSQL interprets `NUMBER(38)`
-- as an Integer domain.

CREATE TABLE "integer" (
    "code"          VARCHAR2(32) PRIMARY KEY,
    "number_38"     NUMBER(38)
);

INSERT INTO "integer" ("code", "number_38") VALUES
    ('1-regular', 60);
INSERT INTO "integer" ("code", "number_38") VALUES
    ('2-min', -99999999999999999999999999999999999999);
INSERT INTO "integer" ("code", "number_38") VALUES
    ('3-max', 99999999999999999999999999999999999999);
INSERT INTO "integer" ("code", "number_38") VALUES
    ('4-null', NULL);


-- Binary float types.

CREATE TABLE "float" (
    "code"          VARCHAR2(32) PRIMARY KEY,
    "binary_float"  BINARY_FLOAT,
    "binary_double" BINARY_DOUBLE
);

INSERT INTO "float" ("code", "binary_float", "binary_double") VALUES
    ('1-regular', 271828e-5F, 27182818285e-10D);
INSERT INTO "float" ("code", "binary_float", "binary_double") VALUES
    ('2-small', 1.175494351e-38F, 2.2250738585072014e-308D);
INSERT INTO "float" ("code", "binary_float", "binary_double") VALUES
    ('3-large', 3.402823466e+38F, 1.7976931348623157e+308D);
INSERT INTO "float" ("code", "binary_float", "binary_double") VALUES
    ('4-pinf', '+Inf', '+Inf');
INSERT INTO "float" ("code", "binary_float", "binary_double") VALUES
    ('5-ninf', '-Inf', '-Inf');
INSERT INTO "float" ("code", "binary_float", "binary_double") VALUES
    ('6-nan', 'NaN', 'NaN');
INSERT INTO "float" ("code", "binary_float", "binary_double") VALUES
    ('7-null', NULL, NULL);


-- NUMBER type.


CREATE TABLE "decimal" (
    "code"          VARCHAR2(32) PRIMARY KEY,
    "number"        NUMBER,
    "number_6"      NUMBER(6),
    "number_6_2"    NUMBER(6,2)
);

INSERT INTO "decimal" ("code", "number", "number_6", "number_6_2") VALUES
    ('1-regular', 19953527.34375, 362880, 3543.75);
INSERT INTO "decimal" ("code", "number", "number_6", "number_6_2") VALUES
    ('2-max', NULL, 999999, 9999.99);
INSERT INTO "decimal" ("code", "number", "number_6", "number_6_2") VALUES
    ('3-null', NULL, NULL, NULL);


-- String data types.

CREATE TABLE "string" (
    "code"          VARCHAR2(32) PRIMARY KEY,
    "char"          CHAR,
    "char_8"        CHAR(8),
    "nchar"         NCHAR,
    "nchar_8"       NCHAR(8),
    "varchar2_8"    VARCHAR2(8),
    "nvarchar2_8"   NVARCHAR2(8),
    "clob"          CLOB,
    "nclob"         NCLOB
);

INSERT INTO "string" ("code", "char", "char_8", "nchar", "nchar_8", "varchar2_8", "nvarchar2_8", "clob", "nclob") VALUES
    ('1-regular', 'H', 'HTSQL', 'H', 'HTSQL', 'HTSQL', 'HTSQL', 'HTSQL', 'HTSQL');
INSERT INTO "string" ("code", "char", "char_8", "nchar", "nchar_8", "varchar2_8", "nvarchar2_8", "clob", "nclob") VALUES
    ('2-unicode', NULL, 'λ', 'λ', 'λόγος', 'λ', 'λόγος', 'λόγος', 'λόγος');
INSERT INTO "string" ("code", "char", "char_8", "nchar", "nchar_8", "varchar2_8", "nvarchar2_8", "clob", "nclob") VALUES
    ('3-null', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


-- Oracle has no ENUM data type.

CREATE TABLE "enum" (
    "code"          VARCHAR2(32) PRIMARY KEY
);


-- DateTime types.

CREATE TABLE "datetime" (
    "code"          VARCHAR2(32) PRIMARY KEY,
    "date"          DATE,
    "timestamp"     TIMESTAMP,
    "timestamptz"   TIMESTAMP WITH TIME ZONE,
    "timestampltz"  TIMESTAMP WITH LOCAL TIME ZONE
);

INSERT INTO "datetime" ("code", "date", "timestamp", "timestamptz", "timestampltz") VALUES
    ('1-regular', DATE '2010-04-15', TIMESTAMP '2010-04-15 20:13:04.5', TIMESTAMP '2010-04-15 20:13:04.5 US/Eastern', TIMESTAMP '2010-04-15 20:13:04.5 US/Eastern');
INSERT INTO "datetime" ("code", "date", "timestamp", "timestamptz", "timestampltz") VALUES
    ('2-max', DATE '4712-12-31', TIMESTAMP '4712-12-31 23:59:59.999999999', TIMESTAMP '4712-12-31 23:59:59.999999999', TIMESTAMP '4712-12-31 23:59:59.999999999');
INSERT INTO "datetime" ("code", "date", "timestamp", "timestamptz", "timestampltz") VALUES
    ('3-zero', DATE '0000-01-01', TIMESTAMP '0000-01-01 00:00:00.0', TIMESTAMP '0000-01-01 00:00:00.0', TIMESTAMP '0000-01-01 00:00:00.0');
INSERT INTO "datetime" ("code", "date", "timestamp", "timestamptz", "timestampltz") VALUES
    ('4-null', NULL, NULL, NULL, NULL);


-- Oracle has no native Date type.

CREATE TABLE "date" (
    "code"          VARCHAR2(32) PRIMARY KEY
);


-- Oracle has no native Time type.

CREATE TABLE "time" (
    "code"          VARCHAR2(32) PRIMARY KEY
);


-- Unsupported data types.

CREATE TABLE "other" (
    "code"          VARCHAR2(32) PRIMARY KEY,
    "interval_ym"   INTERVAL YEAR TO MONTH,
    "interval_ds"   INTERVAL DAY TO SECOND,
    "blob"          BLOB,
    "raw"           RAW(100),
    "long_raw"      LONG RAW,
    "xml"           XMLTYPE
);

INSERT INTO "other" ("code", "interval_ym", "interval_ds", "blob", "raw", "long_raw", "xml") VALUES
    ('1-regular', INTERVAL '2-10' YEAR TO MONTH, INTERVAL '4 5:12:54' DAY TO SECOND, '00FF00FF', '00FF00FF', '00FF00FF', '<title>HTSQL</title>');
INSERT INTO "other" ("code", "interval_ym", "interval_ds", "blob", "raw", "long_raw", "xml") VALUES
    ('2-null', NULL, NULL, NULL, NULL, NULL, NULL);


-- Entity Names.

CREATE TABLE "Три Поросенка" (
    -- Oracle appears not to support `"` in names.
    "?Ниф-Ниф?"     NVARCHAR2(16),
    "`Нуф-Нуф`"     NVARCHAR2(16),
    "[Наф-Наф]"     NVARCHAR2(16)
);

INSERT INTO "Три Поросенка" ("?Ниф-Ниф?", "`Нуф-Нуф`", "[Наф-Наф]") VALUES
    ('соломенный', 'деревянный', 'каменный');


