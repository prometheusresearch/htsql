--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- See `LICENSE` for license information, `AUTHORS` for the list of authors.
--


-- The `type` database contains columns of all available types.


-- If the type contains `BOOL`, it is assigned to the Boolean domain.
-- SQLite does not have a native Boolean type, so Boolean values
-- are represented by zero (FALSE) and any non-zero integer (TRUE).

CREATE TABLE "boolean" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY,
    "boolean"       BOOLEAN
);

INSERT INTO "boolean" ("code", "boolean") VALUES
    ('1-true', 1);
INSERT INTO "boolean" ("code", "boolean") VALUES
    ('2-false', 0);
INSERT INTO "boolean" ("code", "boolean") VALUES
    ('3-true-nonstandard', -1);
INSERT INTO "boolean" ("code", "boolean") VALUES
    ('4-null', NULL);
INSERT INTO "boolean" ("code", "boolean") VALUES
    ('5-invalid-text', '<invalid>');


-- A type containing `INT` is assigned to the Integer domain.

CREATE TABLE "integer" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY,
    "integer"       INTEGER
);

INSERT INTO "integer" ("code", "integer") VALUES
    ('1-regular', 60);
INSERT INTO "integer" ("code", "integer") VALUES
    ('2-min', -9223372036854775808);
INSERT INTO "integer" ("code", "integer") VALUES
    ('3-max', 9223372036854775807);
INSERT INTO "integer" ("code", "integer") VALUES
    ('4-null', NULL);
INSERT INTO "integer" ("code", "integer") VALUES
    ('5-invalid-float', 271828e-5);
INSERT INTO "integer" ("code", "integer") VALUES
    ('6-invalid-text', '<invalid>');


-- A type containing `REAL`, `FLOA` or `DOUB` is assigned to
-- the Float domain.

CREATE TABLE "float" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY,
    "real"          REAL
);

INSERT INTO "float" ("code", "real") VALUES
    ('1-regular', 271828e-5);
INSERT INTO "float" ("code", "real") VALUES
    ('2-small', 2.2250738585072014e-308);
INSERT INTO "float" ("code", "real") VALUES
    ('3-large', 1.7976931348623157e+308);
INSERT INTO "float" ("code", "real") VALUES
    ('4-null', NULL);
INSERT INTO "float" ("code", "real") VALUES
    ('6-invalid-pinf', 1e+309);
INSERT INTO "float" ("code", "real") VALUES
    ('7-invalid-ninf', -1e+309);
INSERT INTO "float" ("code", "real") VALUES
    ('8-invalid-text', '<invalid>');


-- SQLite does not support the Decimal domain.

CREATE TABLE "decimal" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY
);


-- A type containing `CHAR`, `CLOB` or `TEXT` is assigned
-- to the String domain.  This respects SQLite affinity
-- rules for `TEXT` storage class.

CREATE TABLE "string" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY,
    "text"          TEXT
);

INSERT INTO "string" ("code", "text") VALUES
    ('1-regular', 'HTSQL');
INSERT INTO "string" ("code", "text") VALUES
    ('2-unicode', x'CEBBCF8CCEB3CEBFCF82');
INSERT INTO "string" ("code", "text") VALUES
    ('3-empty', '');
INSERT INTO "string" ("code", "text") VALUES
    ('4-null', NULL);
INSERT INTO "string" ("code", "text") VALUES
    ('5-invalid-nil', x'00');
INSERT INTO "string" ("code", "text") VALUES
    ('6-invalid-binary', x'FF');


-- SQLite engine does not support Enum domain.

CREATE TABLE "enum" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY
);


-- A type containing `DATETIME` or `TIMESTAMP` is assigned to the DateTime
-- domain.  In SQLite, DateTime values are represented as a string of the
-- form: `YYYY-MM-DD hh:mm:ss`.

CREATE TABLE "datetime" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY,
    "datetime"      DATETIME
);

INSERT INTO "datetime" ("code", "datetime") VALUES
    ('1-regular', '2010-04-15 20:13:04.5');
INSERT INTO "datetime" ("code", "datetime") VALUES
    ('2-null', NULL);
INSERT INTO "datetime" ("code", "datetime") VALUES
    ('3-invalid-range', DATETIME('-1000-01-01'));
INSERT INTO "datetime" ("code", "datetime") VALUES
    ('4-invalid-value', '2000-13-32 25:00:00');
INSERT INTO "datetime" ("code", "datetime") VALUES
    ('5-invalid-text', '<invalid>');


-- A type containing `DATE` is assigned to the Date domain.  Date values
-- are represented as a string of the form: `YYYY-MM-DD`.

CREATE TABLE "date" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY,
    "date"          DATE
);

INSERT INTO "date" ("code", "date") VALUES
    ('1-regular', '2010-04-15');
INSERT INTO "date" ("code", "date") VALUES
    ('2-null', NULL);
INSERT INTO "date" ("code", "date") VALUES
    ('3-invalid-range', DATE('-1000-01-01'));
INSERT INTO "date" ("code", "date") VALUES
    ('4-invalid-value', '25:00:00');
INSERT INTO "date" ("code", "date") VALUES
    ('5-invalid-text', '<invalid>');


-- A type containing `TIME` is assigned to the Time domain.  Time values
-- are represented as a string of the form: `hh:mm:ss.sss`.

CREATE TABLE "time" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY,
    "time"          TIME
);

INSERT INTO "time" ("code", "time") VALUES
    ('1-regular', '20:13:04.5');
INSERT INTO "time" ("code", "time") VALUES
    ('2-null', NULL);
INSERT INTO "time" ("code", "time") VALUES
    ('3-invalid-value', '25:00:00');
INSERT INTO "time" ("code", "time") VALUES
    ('4-invalid-text', '<invalid>');


-- Any unrecognized data types are assigned to the Opaque domain.

CREATE TABLE "other" (
    "code"          VARCHAR(32) NOT NULL PRIMARY KEY,
    "blob"          BLOB
);

INSERT INTO "other" ("code", "blob") VALUES
    ('1-regular', 'BLOB');
INSERT INTO "other" ("code", "blob") VALUES
    ('2-special', x'0102030405060708090A0B0C0D0E0F');
INSERT INTO "other" ("code", "blob") VALUES
    ('3-nil', x'000000000000000000000000000000');
INSERT INTO "other" ("code", "blob") VALUES
    ('4-binary', x'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFF');
INSERT INTO "other" ("code", "blob") VALUES
    ('5-empty', x'');
INSERT INTO "other" ("code", "blob") VALUES
    ('6-null', NULL);


