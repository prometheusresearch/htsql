-- TODO: HTSQL regression schema for PostgreSQL.

CREATE TABLE test (
    id      INTEGER,
    data    VARCHAR(16),
    CONSTRAINT test_pk PRIMARY KEY (id),
    CONSTRAINT test_data_uk UNIQUE (data)
);

INSERT INTO test (id, data) VALUES (1, 'one');
INSERT INTO test (id, data) VALUES (2, 'two');
INSERT INTO test (id, data) VALUES (3, 'three');

