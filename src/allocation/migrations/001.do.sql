CREATE SCHEMA IF NOT EXISTS allocation;
SET search_path TO allocation;


CREATE TABLE IF NOT EXISTS batches (
    id                  serial      NOT NULL,
    batch_ref           varchar     NOT NULL,
    sku                 varchar     NOT NULL,
    qty                 integer     NOT NULL,
    eta                 timestamp,
    CHECK ( qty >= 0 ),
    UNIQUE ( batch_ref ),
    PRIMARY KEY ( id )
);


COMMENT ON TABLE batches IS 'Batches';
COMMENT ON COLUMN batches.id IS 'Batch identifier';
COMMENT ON COLUMN batches.batch_ref IS 'Batch reference';
COMMENT ON COLUMN batches.sku IS 'Stock-keeping unit, identifier of product';
COMMENT ON COLUMN batches.qty IS 'Purchased quantity';
COMMENT ON COLUMN batches.eta IS 'Estimated arrival time';


CREATE TABLE IF NOT EXISTS order_lines (
    id          serial  NOT NULL,
    sku         varchar NOT NULL,
    qty         integer NOT NULL,
    order_ref   varchar NOT NULL,
    CHECK ( qty >= 0 ),
    PRIMARY KEY ( id )
);


COMMENT ON TABLE order_lines IS 'Order lines';
COMMENT ON COLUMN order_lines.id IS 'Order line identifier';
COMMENT ON COLUMN order_lines.sku IS 'Stock-keeping unit, identifier of product';
COMMENT ON COLUMN order_lines.qty IS 'Quantity of ordered';
COMMENT ON COLUMN order_lines.order_ref IS 'Order reference';


CREATE TABLE IF NOT EXISTS allocations (
    id               serial  NOT NULL,
    order_line_id    integer NOT NULL,
    batch_id         integer NOT NULL,
    PRIMARY KEY ( id ),
    FOREIGN KEY ( order_line_id ) REFERENCES order_lines ( id ),
    FOREIGN KEY ( batch_id ) REFERENCES batches ( id )
);


COMMENT ON TABLE allocations IS 'Allocations';
COMMENT ON COLUMN allocations.id IS 'Allocation identifier';
COMMENT ON COLUMN allocations.order_line_id IS 'Order line identifier';
COMMENT ON COLUMN allocations.batch_id IS 'Batch identifier';
