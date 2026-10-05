-- 02_app_ref.sql: builds the "live shop" (app), lookups (ref), etl. Run once.
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE app.orders         (LIKE seed.orders         INCLUDING ALL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE app.order_items    (LIKE seed.order_items    INCLUDING ALL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE app.order_payments (LIKE seed.order_payments INCLUDING ALL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE app.order_reviews  (LIKE seed.order_reviews  INCLUDING ALL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE app.customers      (LIKE seed.customers      INCLUDING ALL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE app.sellers        (LIKE seed.sellers        INCLUDING ALL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE app.products       (LIKE seed.products       INCLUDING ALL, updated_at timestamptz NOT NULL DEFAULT now());

INSERT INTO app.sellers  SELECT *, now() FROM seed.sellers;
INSERT INTO app.products SELECT *, now() FROM seed.products;

CREATE OR REPLACE FUNCTION etl.touch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := now(); RETURN NEW; END $$;

DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['orders','order_items','order_payments','order_reviews','customers','sellers','products'] LOOP
    EXECUTE format('CREATE TRIGGER trg_touch BEFORE UPDATE ON app.%I FOR EACH ROW EXECUTE FUNCTION etl.touch()', t);
    EXECUTE format('CREATE INDEX ON app.%I (updated_at)', t);
  END LOOP;
END $$;

CREATE TABLE ref.ref_order_status (status_code text PRIMARY KEY, description text, sequence int, is_terminal boolean, updated_at timestamptz DEFAULT now());
INSERT INTO ref.ref_order_status (status_code, description, sequence, is_terminal) VALUES
 ('created','Order placed',1,false), ('approved','Payment approved',2,false), ('invoiced','Invoice issued',3,false),
 ('processing','Being prepared',4,false), ('shipped','Handed to carrier',5,false), ('delivered','Delivered',6,true),
 ('canceled','Canceled',7,true), ('unavailable','Item unavailable',8,true);

CREATE TABLE ref.ref_payment_type (payment_type text PRIMARY KEY, description text, is_card boolean, updated_at timestamptz DEFAULT now());
INSERT INTO ref.ref_payment_type (payment_type, description, is_card) VALUES
 ('credit_card','Credit card',true), ('debit_card','Debit card',true), ('boleto','Boleto bank slip',false),
 ('voucher','Voucher',false), ('not_defined','Not defined',false);

CREATE TABLE ref.ref_state_region (state_code text PRIMARY KEY, state_name text, region text, updated_at timestamptz DEFAULT now());
INSERT INTO ref.ref_state_region (state_code, state_name, region) VALUES
 ('AC','Acre','North'), ('AP','Amapa','North'), ('AM','Amazonas','North'), ('PA','Para','North'),
 ('RO','Rondonia','North'), ('RR','Roraima','North'), ('TO','Tocantins','North'),
 ('AL','Alagoas','Northeast'), ('BA','Bahia','Northeast'), ('CE','Ceara','Northeast'), ('MA','Maranhao','Northeast'),
 ('PB','Paraiba','Northeast'), ('PE','Pernambuco','Northeast'), ('PI','Piaui','Northeast'),
 ('RN','Rio Grande do Norte','Northeast'), ('SE','Sergipe','Northeast'),
 ('DF','Distrito Federal','Central-West'), ('GO','Goias','Central-West'), ('MT','Mato Grosso','Central-West'),
 ('MS','Mato Grosso do Sul','Central-West'),
 ('ES','Espirito Santo','Southeast'), ('MG','Minas Gerais','Southeast'), ('RJ','Rio de Janeiro','Southeast'), ('SP','Sao Paulo','Southeast'),
 ('PR','Parana','South'), ('RS','Rio Grande do Sul','South'), ('SC','Santa Catarina','South');

CREATE TABLE ref.ref_product_category (category_pt text PRIMARY KEY, category_en text, category_group text, updated_at timestamptz DEFAULT now());
INSERT INTO ref.ref_product_category (category_pt, category_en)
  SELECT product_category_name, product_category_name_english FROM seed.category_translation;

CREATE TABLE ref.ref_geolocation AS
  SELECT geolocation_zip_code_prefix AS zip_prefix, min(geolocation_city) AS city, min(geolocation_state) AS state,
         avg(geolocation_lat) AS lat, avg(geolocation_lng) AS lng, now() AS updated_at
  FROM seed.geolocation GROUP BY 1;
ALTER TABLE ref.ref_geolocation ADD PRIMARY KEY (zip_prefix);
