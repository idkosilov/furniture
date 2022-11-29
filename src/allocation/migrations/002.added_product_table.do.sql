CREATE TABLE IF NOT EXISTS products (
    sku varchar NOT NULL,
    PRIMARY KEY ( sku )
);


COMMENT ON TABLE products IS 'Products';
COMMENT ON COLUMN products.sku IS 'Stock-keeping unit';

ALTER TABLE batches ADD CONSTRAINT fk_batches_products FOREIGN KEY ( sku ) REFERENCES products ( sku )