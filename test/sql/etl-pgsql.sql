--
-- Copyright (c) 2006-2012, Prometheus Research, LLC
--

-- --------------------------------------------------------------------
-- A regression schema to test CRUD and ETL processes for PostgreSQL
--

CREATE TABLE manufacturer (
    code                VARCHAR(16) NOT NULL,
    name                VARCHAR(64) NOT NULL,
    CONSTRAINT manufacturer_pk
      PRIMARY KEY (code),
    CONSTRAINT manufacturer_name_uk
      UNIQUE (name)
);

CREATE TABLE product_line (
    manufacturer_code   VARCHAR(16) NOT NULL,
    code                VARCHAR(16) NOT NULL,
    title               VARCHAR(64),
    CONSTRAINT product_line_pk
      PRIMARY KEY (manufacturer_code, code),
    CONSTRAINT product_line_title_uk
      UNIQUE (manufacturer_code, title),
    CONSTRAINT product_line_manufacturer_fk
      FOREIGN KEY (manufacturer_code)
      REFERENCES manufacturer(code)
);

CREATE TABLE product (
    sku                 CHAR(8) NOT NULL,
    manufacturer_code   VARCHAR(16) NOT NULL,
    product_line_code   VARCHAR(16),
    title               VARCHAR(64) NOT NULL,
    is_available        BOOLEAN NOT NULL DEFAULT TRUE,
    description         TEXT,
    list_price          DECIMAL(8,2),
    CONSTRAINT product_pk
      PRIMARY KEY (sku),
    CONSTRAINT product_manufacturer_fk
      FOREIGN KEY (manufacturer_code)
      REFERENCES manufacturer(code),
    CONSTRAINT product_product_line_fk
      FOREIGN KEY (manufacturer_code, product_line_code)
      REFERENCES product_line(manufacturer_code, code)
);

CREATE TABLE category (
    id                  SERIAL NOT NULL,
    label               TEXT NOT NULL,
    parent_id           INTEGER,
    CONSTRAINT category_pk
      PRIMARY KEY (id),
    CONSTRAINT category_uk
      UNIQUE (label),
    CONSTRAINT category_parent_fk
      FOREIGN KEY (parent_id)
      REFERENCES category(id)
);

CREATE TABLE product_category (
    product_sku         CHAR(8) NOT NULL,
    category_id         INTEGER NOT NULL,
    CONSTRAINT product_category_pk
      PRIMARY KEY (product_sku, category_id),
    CONSTRAINT product_category_product_fk
      FOREIGN KEY (product_sku)
      REFERENCES product(sku),
    CONSTRAINT product_category_category_fk
      FOREIGN KEY (category_id)
      REFERENCES category(id)
);

CREATE TABLE customer (
    handle              VARCHAR(16) NOT NULL, -- User-provided identifier
    guid                CHAR(36) NOT NULL,    -- 128-bit as 32 hex with 4 hyphens
    CONSTRAINT customer_pk
      PRIMARY KEY (handle),
    CONSTRAINT customer_uk
      UNIQUE (guid)
);

CREATE TABLE customer_info (
    customer_guid       CHAR(36) NOT NULL,
    email_address       VARCHAR(128) NOT NULL,
    given_name          VARCHAR(32),
    family_name         VARCHAR(32),
    CONSTRAINT customer_info_pk
      PRIMARY KEY (customer_guid),
    CONSTRAINT customer_info_customer_fk
      FOREIGN KEY (customer_guid)
      REFERENCES customer(guid)
);

CREATE TABLE "order" (
    id                  SERIAL NOT NULL,
    customer_guid       CHAR(36) NOT NULL,
    "date"              DATE NOT NULL DEFAULT CURRENT_DATE,
    CONSTRAINT order_pk
      PRIMARY KEY (id),
    CONSTRAINT order_customer_fk
      FOREIGN KEY (customer_guid)
      REFERENCES customer(guid)
);

CREATE TABLE order_line (
    order_id            INTEGER NOT NULL,
    no                  INTEGER NOT NULL,
    product_sku         CHAR(8) NOT NULL,
    unit_price          DECIMAL(8,2) NOT NULL,
    quantity            INTEGER NOT NULL,
    CONSTRAINT order_line_pk
      PRIMARY KEY (order_id, no),
    CONSTRAINT order_line_order_fk
      FOREIGN KEY (order_id)
      REFERENCES "order"(id),
    CONSTRAINT order_line_product_fk
      FOREIGN KEY (product_sku)
      REFERENCES product(sku)
);

CREATE TYPE order_status_code_t AS
   ENUM ('pending', 'charged', 'shipped', 'challenged');

CREATE TABLE order_status (
    order_id            INTEGER NOT NULL,
    "update"            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status_code         order_status_code_t NOT NULL,
    comments            TEXT,
    CONSTRAINT order_status_pk
      PRIMARY KEY (order_id, "update"),
    CONSTRAINT order_status_order_fk
      FOREIGN KEY (order_id)
      REFERENCES "order"(id)
);

