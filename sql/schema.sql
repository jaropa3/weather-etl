-- =============================================================
-- schema.sql
-- =============================================================

CREATE TABLE stag (
    item_name       text         NOT NULL,
    description     text         NOT NULL,
    qty             smallint     NOT NULL,
    store_name      text         NOT NULL
);

CREATE TABLE dim_item (
    item_id     smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_name   text NOT NULL,
    description text NOT NULL,
    CONSTRAINT dim_item_name_unique UNIQUE (item_name)
);

CREATE TABLE dim_store_name (
    store_id   smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_name text NOT NULL,
    CONSTRAINT dim_store_name_unique UNIQUE (store_name)
);

CREATE TABLE final_merge (
    id              bigint       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_id       smallint       NOT NULL,
    qty             smallint     NOT NULL CHECK (qty > 0),
    store_id      smallint       NOT NULL,
    created_at      timestamp    DEFAULT now() NOT NULL,
    CONSTRAINT fk_item
        FOREIGN KEY (item_id)
        REFERENCES dim_item(item_id),

    CONSTRAINT fk_store
        FOREIGN KEY (store_id)
        REFERENCES dim_store_name(store_id)
);

   