--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- See `LICENSE` for license information, `AUTHORS` for the list of authors.
--


-- The `edge` database contains testing data for some edge cases.


-- Data Types and Values.

CREATE SCHEMA "type";

-- `BOOL` represents a Boolean type with two values: `TRUE` and `FALSE`.

CREATE TABLE "type"."boolean" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "bool"          BOOLEAN
);

INSERT INTO "type"."boolean" ("code", "bool") VALUES
    ('1-true', TRUE),
    ('2-false', FALSE),
    ('3-null', NULL);


-- HTSQL recognizes the following numeric types:
-- - INT2, INT4, INT8 are assigned to the Integer domain;
-- - FLOAT4, FLOAT8 are assigned to the Float domain;
-- - NUMERIC is assigned to the Decimal domain.

-- Types `INT2`, `INT4` and `INT8` are recognized as Integer.

CREATE TABLE "type"."integer" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "int2"          SMALLINT,
    "int4"          INTEGER,
    "int8"          BIGINT
);

INSERT INTO "type"."integer" ("code", "int2", "int4", "int8") VALUES
    ('1-regular', 60, 3600, 12960000),
    ('2-min', -32768, -2147483648, -9223372036854775808),
    ('3-max', +32767, +2147483647, +9223372036854775807),
    ('4-null', NULL, NULL, NULL);


-- Types `FLOAT4` and `FLOAT8` are recognized as Float.

CREATE TABLE "type"."float" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "float4"        REAL,
    "float8"        DOUBLE PRECISION
);

INSERT INTO "type"."float" ("code", "float4", "float8") VALUES
    ('1-regular', 271828e-5, 27182818285e-10),
    ('2-small', 1.175494351e-38, 2.2250738585072014e-308),
    ('3-large', 3.402823466e+38, 1.797693134862315e+308),
    ('4-pinf', 'Infinity', 'Infinity'),
    ('5-ninf', '-Infinity', '-Infinity'),
    ('6-nan', 'NaN', 'NaN'),
    ('7-null', NULL, NULL);


-- The `NUMERIC` type is recognized as Decimal.

CREATE TABLE "type"."decimal" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "numeric"       NUMERIC,
    "numeric_6"     NUMERIC(6),
    "numeric_6_2"   NUMERIC(6,2)
);

INSERT INTO "type"."decimal" ("code", "numeric", "numeric_6", "numeric_6_2") VALUES
    ('1-regular', 19953527.34375, 362880, 3543.75),
    ('2-max', NULL, 999999, 9999.99),
    ('3-nan', 'NaN', 'NaN', 'NaN'),
    ('4-null', NULL, NULL, NULL);


-- `BPCHAR`, `VARCHAR` and `TEXT` are recognized as String.

CREATE TABLE "type"."string" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "bpchar"        CHAR,
    "bpchar_8"      CHAR(8),
    "varchar"       VARCHAR,
    "varchar_8"     VARCHAR(8),
    "text"          TEXT
);

INSERT INTO "type"."string" ("code", "bpchar", "bpchar_8", "varchar", "varchar_8", "text") VALUES
    ('1-regular', 'H', 'HTSQL', 'HTSQL', 'HTSQL', 'HTSQL'),
    ('2-unicode', E'\xCE\xBB', E'\xce\xbb\xcf\x8c\xce\xb3\xce\xbf\xcf\x82', E'\xce\xbb\xcf\x8c\xce\xb3\xce\xbf\xcf\x82', E'\xce\xbb\xcf\x8c\xce\xb3\xce\xbf\xcf\x82', E'\xce\xbb\xcf\x8c\xce\xb3\xce\xbf\xcf\x82'),
    ('3-special', E'\x01', E'\x01\x02\x03\x04', E'\x01\x02\x03\x04', E'\x01\x02\x03\x04', E'\x01\x02\x03\x04'),
    ('4-empty', '', '', '', '', ''),
    ('5-null', NULL, NULL, NULL, NULL, NULL);


-- PostgreSQL supports ENUM types.

CREATE TYPE ENUM1 AS ENUM ('one', 'two');
CREATE TYPE ENUM2 AS ENUM ('one', 'two', 'three');

CREATE TABLE "type"."enum" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "enum1"         ENUM1,
    "enum2"         ENUM2
);

INSERT INTO "type"."enum" ("code", "enum1", "enum2") VALUES
    ('1-regular', 'one', 'two'),
    ('2-null', NULL, NULL);


-- TIMESTAMP and TIMESTAMPTZ values are stored as a Julian day in UTC.
-- TIMESTAMPTZ values are converted from the server time zone on input
-- and to the server time zone on output.

CREATE TABLE "type"."datetime" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "timestamp"     TIMESTAMP,
    "timestamp_0"   TIMESTAMP(0),
    "timestamptz"   TIMESTAMP WITH TIME ZONE,
    "timestamptz_0" TIMESTAMP(0) WITH TIME ZONE
);

INSERT INTO "type"."datetime" ("code", "timestamp", "timestamp_0", "timestamptz", "timestamptz_0") VALUES
    ('1-regular', '2010-04-15 20:13:04.5', '2010-04-15 20:13:04', '2010-04-15 20:13:04.5 EDT', '2010-04-15 20:13:04 EDT'),
    ('2-min', '4714-12-31 00:00:00.0 BC', '4714-12-31 00:00:00 BC', '4714-11-24 00:00:00.0 BC UTC', '4714-11-24 00:00:00 BC UTC'),
    ('3-max', '294277-01-09 04:00:54.7', '294277-01-09 04:00:54', '294277-01-09 04:00:54.7 UTC', '294277-01-09 04:00:54 UTC'),
    ('4-pinf', 'infinity', 'infinity', 'infinity', 'infinity'),
    ('5-ninf', '-infinity', '-infinity', '-infinity', '-infinity'),
    ('6-null', NULL, NULL, NULL, NULL);



-- In PostgreSQL, DATE values are stored as a Julian day.

CREATE TABLE "type"."date" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "date"          DATE
);

INSERT INTO "type"."date" ("code", "date") VALUES
    ('1-regular', '2010-04-15'),
    ('2-min', '4714-11-24 BC'),
    ('3-max', '5874897-12-31'),
    ('4-pinf', 'infinity'),
    ('5-ninf', '-infinity'),
    ('6-null', NULL);


-- TIME data types (TIMETZ is not included because of its limited usefullness).

CREATE TABLE "type"."time" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "time"          TIME,
    "time_0"        TIME(0)
);

INSERT INTO "type"."time" ("code", "time", "time_0") VALUES
    ('1-regular', '20:13:04.5', '20:13:04'),
    ('2-min', '00:00:00.0', '00:00:00'),
    ('3-max', '24:00:00.0', '24:00:00'),
    ('4-null', NULL, NULL);


-- Unsupported data types (non-exhaustive list).

CREATE TYPE COMPOSITE AS (
    "text"          TEXT,
    "integer"       INTEGER,
    "null"          TEXT
);

CREATE TABLE "type"."other" (
    "code"          VARCHAR(32) PRIMARY KEY,
    "money"         MONEY,
    "bytea"         BYTEA,
    "interval"      INTERVAL,
    "inet"          INET,
    "uuid"          UUID,
    "xml"           XML,
    "integer_array" INTEGER[],
    "text_array2"   TEXT[][],
    "composite"     COMPOSITE
);

INSERT INTO "type"."other" ("code", "money", "bytea", "interval", "inet", "uuid", "xml", "integer_array", "text_array2", "composite") VALUES
    ('1-regular', '$1,000.00', E'\\000\\377\\000\\377', '1 year 2 months 3 days 04:05:06', '127.0.0.1', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '<title>HTSQL</title>', '{1,2,3,NULL}', '{{A,B,C,NULL},{alpha,beta,gamma,NULL}}', '(HTSQL,42,)'),
    ('2-null', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


