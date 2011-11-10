--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- See `LICENSE` for license information, `AUTHORS` for the list of authors.
--


-- The `edge` database contains testing data for some edge cases.


-- Data Types and Values.


-- In MySQL, BOOLEAN type is an alias for TINYINT(1); TRUE and FALSE constants
-- are aliases for 1 and 0 respectively.

CREATE TABLE `boolean` (
    `code`          VARCHAR(32) PRIMARY KEY,
    `tinyint_1`     BOOLEAN
) ENGINE=INNODB;

INSERT INTO `boolean` (`code`, `tinyint_1`) VALUES
    ('1-true', TRUE),
    ('2-false', FALSE),
    ('3-true-nonstandard', -1),
    ('4-null', NULL);


-- MySQL supports 1-, 2-, 3-, 4-, and 8-byte integer types; signed and
-- unsigned.

CREATE TABLE `integer` (
    `code`          VARCHAR(32) PRIMARY KEY,
    `tinyint`       TINYINT,
    `tinyint_u`     TINYINT UNSIGNED,
    `smallint`      SMALLINT,
    `smallint_u`    SMALLINT UNSIGNED,
    `mediumint`     MEDIUMINT,
    `mediumint_u`   MEDIUMINT UNSIGNED,
    `int`           INTEGER,
    `int_u`         INTEGER UNSIGNED,
    `bigint`        BIGINT,
    `bigint_u`      BIGINT UNSIGNED
) ENGINE=INNODB;

INSERT INTO `integer` (`code`, `tinyint`, `tinyint_u`, `smallint`, `smallint_u`, `mediumint`, `mediumint_u`, `int`, `int_u`, `bigint`, `bigint_u`) VALUES
    ('1-regular', -60, 60, -3600, 3600, -216000, 216000, -12960000, 12960000, -167961600000000, 167961600000000),
    ('2-min', -128, 0, -32768, 0, -8388608, 0, -2147483648, 0, -9223372036854775808, 0),
    ('3-max', 127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 9223372036854775807, 18446744073709551615),
    ('4-null', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


-- MySQL supports single- and double-precision float values; signed and
-- unsigned.

CREATE TABLE `float` (
    `code`          VARCHAR(32) PRIMARY KEY,
    `float`         FLOAT,
    `float_u`       FLOAT UNSIGNED,
    `double`        DOUBLE,
    `double_u`      DOUBLE UNSIGNED
) ENGINE=INNODB;

INSERT INTO `float` (`code`, `float`, `float_u`, `double`, `double_u`) VALUES
    ('1-regular', -271828e-5, 271828e-5, -27182818285e-10, 27182818285e-10),
    ('2-small', 1.175494351e-38, 1.175494351e-38, 2.2250738585072014e-308, 2.2250738585072014e-308),
    ('3-large', 3.402823466e+38, 3.402823466e+38, 1.797693134862315e+308, 1.797693134862315e+308),
    ('4-null', NULL, NULL, NULL, NULL);


-- DECIMAL(M,D) represents exact decimal numbers.

CREATE TABLE `decimal` (
    `code`          VARCHAR(32) PRIMARY KEY,
    `decimal`       DECIMAL,
    `decimal_u`     DECIMAL UNSIGNED,
    `decimal_65_30` DECIMAL(65,30),
    `decimal_6`     DECIMAL(6),
    `decimal_6_2`   DECIMAL(6,2)
) ENGINE=INNODB;

INSERT INTO `decimal` (`code`, `decimal`, `decimal_u`, `decimal_65_30`, `decimal_6`, `decimal_6_2`) VALUES
    ('1-regular', -479001600, 479001600, 19953527.34375, 362880, 3543.75),
    ('2-max', 9999999999, 9999999999, 99999999999999999999999999999999999.999999999999999999999999999999, 999999, 9999.99),
    ('3-null', NULL, NULL, NULL, NULL, NULL);


-- MySQL string types.

CREATE TABLE `string` (
    `code`          VARCHAR(32) PRIMARY KEY,
    `char`          CHAR,
    `char_8`        CHAR(8),
    `varchar_8`     VARCHAR(8),
    `tinytext`      TINYTEXT,
    `text`          TEXT,
    `mediumtext`    MEDIUMTEXT,
    `longtext`      LONGTEXT,
    `text_latin1`   TEXT CHARACTER SET latin1
) ENGINE=INNODB;

INSERT INTO `string` (`code`, `char`, `char_8`, `varchar_8`, `tinytext`, `text`, `mediumtext`, `longtext`, `text_latin1`) VALUES
    ('1-regular', 'H', 'HTSQL', 'HTSQL', 'HTSQL', 'HTSQL', 'HTSQL', 'HTSQL', 'HTSQL'),
    ('2-unicode', 'λ', 'λόγος', 'λόγος', 'λόγος', 'λόγος', 'λόγος', 'λόγος', 'λόγος'),
    ('3-special', '\0', '\0\0\0\0', '\0\0\0\0', '\0\0\0\0', '\0\0\0\0', '\0\0\0\0', '\0\0\0\0', '\0\0\0\0'),
    ('4-empty', '', '', '', '', '', '', '', ''),
    ('5-null', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- ENUM types.

CREATE TABLE `enum` (
    `code`          VARCHAR(32) PRIMARY KEY,
    enum1           ENUM('one', 'two'),
    enum2           ENUM('one', 'two', 'three')
) ENGINE=INNODB;

INSERT INTO `enum` (`code`, `enum1`, `enum2`) VALUES
    ('1-regular', 'one', 'two'),
    ('2-null', NULL, NULL),
    ('3-invalid-value', '<invalid>', '<invalid>');


-- DATETIME and TIMESTAMP values.

CREATE TABLE `datetime` (
    `code`          VARCHAR(32) PRIMARY KEY,
    `datetime`      DATETIME,
    `timestamp`     TIMESTAMP
) ENGINE=INNODB;

INSERT INTO `datetime` (`code`, `datetime`, `timestamp`) VALUES
    ('1-regular', '2010-04-15 20:13:04', '2010-04-15 20:13:04'),
    ('2-min', '1000-01-01 00:00:00', '1970-01-01 00:00:01'),
    ('3-max', '9999-12-31 23:59:59', '2038-01-18 22:14:07'),
    ('4-zero', '0000-00-00 00:00:00', '0000-00-00 00:00:00'),
    ('5-null', NULL, 0);


-- DATE values.

CREATE TABLE `date` (
    `code`          VARCHAR(32) PRIMARY KEY,
    `date`          DATE
) ENGINE=INNODB;

INSERT INTO `date` (`code`, `date`) VALUES
    ('1-regular', '2010-04-15'),
    ('2-min', '1000-01-01'),
    ('3-max', '9999-12-31'),
    ('4-zero', '0000-00-00'),
    ('5-null', NULL);

-- TIME values.

CREATE TABLE `time` (
    `code`          VARCHAR(32) PRIMARY KEY,
    `time`          TIME
) ENGINE=INNODB;

INSERT INTO `time` (`code`, `time`) VALUES
    ('1-regular', '20:13:04'),
    ('2-min', '-838:59:59'),
    ('3-max', '838:59:59'),
    ('4-zero', '00:00:00'),
    ('5-null', NULL);


-- Unsupported data types.

CREATE TABLE `other` (
    `code`          VARCHAR(32) PRIMARY KEY,
    `bit_8`         BIT(8),
    `year_4`        YEAR(4),
    `binary_8`      BINARY(8),
    `blob`          BLOB,
    `set`           SET('one', 'two', 'three', 'four')
) ENGINE=INNODB;

INSERT INTO `other` (`code`, `bit_8`, `year_4`, `binary_8`, `blob`, `set`) VALUES
    ('1-regular', b'10101010', 2010, '\0\0\0\0', '\0\0\0\0', 'two,four'),
    ('2-zero', b'00000000', 0, '', '', ''),
    ('3-null', NULL, NULL, NULL, NULL, NULL);


