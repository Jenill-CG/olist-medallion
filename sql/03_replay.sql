-- 03_replay.sql: makes the Olist data "move".
-- Each tick copies the next 80 hours of history from seed.* into app.*
-- Only runs 10:00–19:00 IST. Scheduled separately with pg_cron.

CREATE TABLE IF NOT EXISTS etl.replay_state (
  id int PRIMARY KEY, sim_ts timestamp NOT NULL, step interval NOT NULL, end_ts timestamp NOT NULL);
INSERT INTO etl.replay_state VALUES (1, '2017-01-01', '80 hours', '2018-10-20')
ON CONFLICT (id) DO NOTHING;

CREATE OR REPLACE FUNCTION etl.replay_tick() RETURNS text LANGUAGE plpgsql AS $$
DECLARE s etl.replay_state; w_from timestamp; w_to timestamp; n int;
BEGIN
  IF (now() AT TIME ZONE 'Asia/Kolkata')::time NOT BETWEEN '10:00' AND '18:59' THEN
    RETURN 'outside working hours';
  END IF;

  SELECT * INTO s FROM etl.replay_state WHERE id = 1 FOR UPDATE;
  IF s.sim_ts >= s.end_ts THEN RETURN 'replay finished'; END IF;
  w_from := s.sim_ts;  w_to := s.sim_ts + s.step;

  -- 1. orders placed in this window (+ customers, items, payments)
  DROP TABLE IF EXISTS new_orders;
  CREATE TEMP TABLE new_orders AS
    SELECT * FROM seed.orders
    WHERE order_purchase_timestamp >= w_from AND order_purchase_timestamp < w_to;

  INSERT INTO app.customers SELECT c.*, now() FROM seed.customers c
    JOIN new_orders o ON o.customer_id = c.customer_id ON CONFLICT (customer_id) DO NOTHING;

  INSERT INTO app.orders (order_id, customer_id, order_status, order_purchase_timestamp, order_estimated_delivery_date, updated_at)
    SELECT order_id, customer_id, 'created', order_purchase_timestamp, order_estimated_delivery_date, now()
    FROM new_orders ON CONFLICT (order_id) DO NOTHING;
  GET DIAGNOSTICS n = ROW_COUNT;

  INSERT INTO app.order_items    SELECT i.*, now() FROM seed.order_items i    JOIN new_orders o USING (order_id) ON CONFLICT DO NOTHING;
  INSERT INTO app.order_payments SELECT p.*, now() FROM seed.order_payments p JOIN new_orders o USING (order_id) ON CONFLICT DO NOTHING;

  -- 2. status changes whose real timestamps fall in this window
  UPDATE app.orders a SET order_status = 'approved', order_approved_at = o.order_approved_at
    FROM seed.orders o WHERE a.order_id = o.order_id AND a.order_status = 'created'
     AND o.order_approved_at >= w_from AND o.order_approved_at < w_to;

  UPDATE app.orders a SET order_status = 'shipped', order_delivered_carrier_date = o.order_delivered_carrier_date
    FROM seed.orders o WHERE a.order_id = o.order_id AND a.order_status IN ('created','approved')
     AND o.order_delivered_carrier_date >= w_from AND o.order_delivered_carrier_date < w_to;

  UPDATE app.orders a SET order_status = 'delivered', order_delivered_customer_date = o.order_delivered_customer_date
    FROM seed.orders o WHERE a.order_id = o.order_id AND a.order_status <> 'delivered'
     AND o.order_delivered_customer_date >= w_from AND o.order_delivered_customer_date < w_to;

  UPDATE app.orders a SET order_status = o.order_status
    FROM seed.orders o WHERE a.order_id = o.order_id
     AND o.order_status IN ('canceled','unavailable','invoiced','processing')
     AND o.order_purchase_timestamp + interval '2 days' >= w_from
     AND o.order_purchase_timestamp + interval '2 days' <  w_to;

  -- 3. reviews written in this window
  INSERT INTO app.order_reviews SELECT r.*, now() FROM seed.order_reviews r
    WHERE r.review_creation_date >= w_from AND r.review_creation_date < w_to
      AND EXISTS (SELECT 1 FROM app.orders a WHERE a.order_id = r.order_id);

  -- 4. occasional master-data changes (~1 tick in 4) for SCD Type 2
  IF random() < 0.25 THEN
    UPDATE app.products SET product_photos_qty = coalesce(product_photos_qty, 0) + 1
      WHERE product_id IN (SELECT product_id FROM app.products ORDER BY random() LIMIT 3);
    UPDATE app.sellers sl SET seller_zip_code_prefix = g.zip_prefix, seller_city = g.city
      FROM (SELECT zip_prefix, city, state FROM ref.ref_geolocation ORDER BY random() LIMIT 1) g
      WHERE sl.seller_id = (SELECT seller_id FROM app.sellers x WHERE x.seller_state = g.state ORDER BY random() LIMIT 1);
  END IF;

  UPDATE etl.replay_state SET sim_ts = w_to WHERE id = 1;
  RETURN format('%s to %s: %s new orders', w_from, w_to, n);
END $$;
