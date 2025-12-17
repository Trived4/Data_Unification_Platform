-- LOCATION
CREATE TABLE dim_location (
    location_sk         INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pos_system          VARCHAR(10) NOT NULL,   -- 'POSA', 'POSB', 'POSC', 'POSD'
    pos_location_id     VARCHAR(64) NOT NULL,   -- original ID in source

    location_code       VARCHAR(64),
    location_name       VARCHAR(255),
    address_line1       VARCHAR(255),
    city                VARCHAR(100),
    state               VARCHAR(50),
    postal_code         VARCHAR(20),
    phone_number        VARCHAR(50),

    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,

    CONSTRAINT uq_dim_location UNIQUE (pos_system, pos_location_id)
);

CREATE INDEX idx_dim_location_code ON dim_location (location_code);

-- CUSTOMER
CREATE TABLE dim_customer (
    customer_sk         INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pos_system          VARCHAR(10) NOT NULL,
    pos_customer_id     VARCHAR(64) NOT NULL,
    customer_guid       VARCHAR(64),

    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(255),
    is_email_valid      BOOLEAN,
    phone_number        VARCHAR(50),

    address_line1       VARCHAR(255),
    city                VARCHAR(100),
    state               VARCHAR(50),
    postal_code         VARCHAR(20),

    is_active           BOOLEAN,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,

    CONSTRAINT uq_dim_customer UNIQUE (pos_system, pos_customer_id)
);

CREATE INDEX idx_dim_customer_email ON dim_customer (email);

-- SERVICE
CREATE TABLE dim_service (
    service_sk          INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pos_system          VARCHAR(10) NOT NULL,
    pos_service_id      VARCHAR(64) NOT NULL,

    service_code        VARCHAR(64),
    service_name        VARCHAR(255),
    service_category    VARCHAR(64),
    base_price          NUMERIC(10, 2),
    is_active           BOOLEAN,

    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,

    CONSTRAINT uq_dim_service UNIQUE (pos_system, pos_service_id)
);

CREATE INDEX idx_dim_service_name ON dim_service (service_name);

-- MEMBERSHIP TIER
CREATE TABLE dim_membership_tier (
    membership_tier_sk  INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pos_system          VARCHAR(10),
    tier_name           VARCHAR(100) NOT NULL,
    tier_description    VARCHAR(255)
);

-- PAYMENT TYPE (optional)
CREATE TABLE dim_payment_type (
    payment_type_sk     INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pos_system          VARCHAR(10),
    pos_payment_type_id VARCHAR(64),
    payment_type_name   VARCHAR(100)
);

-- TRANSACTION HEADER
CREATE TABLE fact_transaction_header (
    transaction_header_sk   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    pos_system              VARCHAR(10) NOT NULL,
    pos_transaction_id      VARCHAR(64) NOT NULL,

    location_sk             INTEGER NOT NULL REFERENCES dim_location(location_sk),
    customer_sk             INTEGER REFERENCES dim_customer(customer_sk),
    membership_tier_sk      INTEGER REFERENCES dim_membership_tier(membership_tier_sk),
    payment_type_sk         INTEGER REFERENCES dim_payment_type(payment_type_sk),

    transaction_ts          TIMESTAMP,
    business_date           DATE,

    subtotal_amount         NUMERIC(12, 2),
    tax_amount              NUMERIC(12, 2),
    discount_amount         NUMERIC(12, 2),
    total_amount            NUMERIC(12, 2),

    is_date_imputed         BOOLEAN DEFAULT FALSE,
    source_row_count        INTEGER DEFAULT 1,

    CONSTRAINT uq_fact_header UNIQUE (pos_system, pos_transaction_id)
);

CREATE INDEX idx_fact_header_location ON fact_transaction_header (location_sk);
CREATE INDEX idx_fact_header_customer ON fact_transaction_header (customer_sk);
CREATE INDEX idx_fact_header_date     ON fact_transaction_header (business_date);

-- LINE ITEMS
CREATE TABLE fact_transaction_line_item (
    transaction_line_sk     BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    transaction_header_sk   BIGINT NOT NULL REFERENCES fact_transaction_header(transaction_header_sk),
    service_sk              INTEGER NOT NULL REFERENCES dim_service(service_sk),

    quantity                NUMERIC(10, 2) NOT NULL,
    unit_price              NUMERIC(12, 2),
    line_amount             NUMERIC(12, 2),

    created_at              TIMESTAMP
);

CREATE INDEX idx_fact_line_service ON fact_transaction_line_item (service_sk);
