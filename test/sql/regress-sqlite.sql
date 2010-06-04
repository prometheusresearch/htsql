-- TODO: HTSQL regression schema for SQLite.

CREATE TABLE test (
    id      INTEGER,
    data    TEXT NOT NULL,
    CONSTRAINT test_pk PRIMARY KEY (id),
    CONSTRAINT test_data_uk UNIQUE (data)
);

INSERT INTO test (id, data) VALUES (1, 'one');
INSERT INTO test (id, data) VALUES (2, 'two');
INSERT INTO test (id, data) VALUES (3, 'three');

