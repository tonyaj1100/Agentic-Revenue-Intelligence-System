-- ====================================================================================
-- Project: Agentic Revenue Intelligence & Supply Chain Optimization System
-- Author: Tony Antony
-- Purpose: End-to-end data pipeline from raw tables to BI-ready analytical views.
-- ROI Focus: Identifying revenue leaks, automating data validation, and flattening 
--            data models to reduce BI compute time and enable Agentic AI querying.
-- ====================================================================================

-- =========================================================
-- PHASE 1: DATABASE SETUP & SCHEMA CREATION
-- Purpose: Building a structured, scalable warehouse.
-- =========================================================

-- Create the isolated analytics database
-- CREATE DATABASE ecommerce_analytics;
-- (Connect to this database before running the below queries)

CREATE SCHEMA IF NOT EXISTS olist;

-- 1. Customers Table
CREATE TABLE olist.customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix INT,
    customer_city TEXT,
    customer_state TEXT
);

-- 2. Orders Table
CREATE TABLE olist.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- 3. Order Items Table
CREATE TABLE olist.order_items (
    order_id TEXT,
    order_item_id INT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10,2), -- Using NUMERIC to prevent float rounding errors in revenue
    freight_value NUMERIC(10,2)
);

-- 4. Order Payments Table
CREATE TABLE olist.order_payments (
    order_id TEXT,
    payment_sequential INT,
    payment_type TEXT,
    payment_installments INT,
    payment_value NUMERIC(10,2)
);

-- 5. Products Table
CREATE TABLE olist.products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

-- 6. Sellers Table
CREATE TABLE olist.sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city TEXT,
    seller_state TEXT
);

-- 7. Order Reviews Table
CREATE TABLE olist.order_reviews (
    review_id TEXT PRIMARY KEY,
    order_id TEXT,
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- 8. Geolocation Table (Optional for heatmaps)
CREATE TABLE olist.geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city TEXT,
    geolocation_state TEXT
);

-- =========================================================
-- PHASE 2: DATA IMPORT (Example)
-- Purpose: Bulk loading data efficiently.
-- =========================================================

/* -- Run these via VS Code or pgAdmin for each table. 
-- Ensure absolute paths are used.
COPY olist.customers
FROM '/absolute/path/to/olist_customers_dataset.csv'
DELIMITER ','
CSV HEADER;
*/

-- =========================================================
-- PHASE 3: DATA VALIDATION & SANITY CHECKS
-- Purpose: Ensuring data integrity to prevent skewed revenue 
--          reporting, saving hours of debugging later.
-- =========================================================

-- 1) Confirm schema exists
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'olist';

-- 2) List expected tables
SELECT table_name FROM information_schema.tables WHERE table_schema = 'olist' ORDER BY table_name;

-- 3) Row counts (core tables)
SELECT
  (SELECT COUNT(*) FROM olist.customers) AS customers,
  (SELECT COUNT(*) FROM olist.orders) AS orders,
  (SELECT COUNT(*) FROM olist.order_items) AS order_items,
  (SELECT COUNT(*) FROM olist.order_payments) AS order_payments,
  (SELECT COUNT(*) FROM olist.order_reviews) AS order_reviews,
  (SELECT COUNT(*) FROM olist.products) AS products,
  (SELECT COUNT(*) FROM olist.sellers) AS sellers,
  (SELECT COUNT(*) FROM olist.geolocation) AS geolocation;

-- 4) Key null checks (identifying dirty data)
SELECT SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS customers_customer_id_nulls FROM olist.customers;
SELECT SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS orders_order_id_nulls FROM olist.orders;
SELECT SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS items_product_id_nulls FROM olist.order_items;

-- 5) Orphan checks (Identifying missing relationships that could break BI models)
-- Order items should match orders
SELECT
  COUNT(*) AS order_items_total,
  COUNT(o.order_id) AS order_items_with_order_match,
  (COUNT(*) - COUNT(o.order_id)) AS order_items_without_order_match
FROM olist.order_items oi
LEFT JOIN olist.orders o ON o.order_id = oi.order_id;

-- Payments should match orders
SELECT
  COUNT(*) AS payments_total,
  COUNT(o.order_id) AS payments_with_order_match,
  (COUNT(*) - COUNT(o.order_id)) AS payments_without_order_match
FROM olist.order_payments op
LEFT JOIN olist.orders o ON o.order_id = op.order_id;

-- 6) Revenue sanity bounds
SELECT ROUND(SUM(payment_value)::numeric, 2) AS total_revenue FROM olist.order_payments;

-- =========================================================
-- PHASE 4: ADVANCED BUSINESS ANALYTICS (Exploratory)
-- Purpose: Queries designed to find actionable ROI insights.
-- =========================================================

-- MoM Revenue Growth using Window Functions (LAG)
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', order_purchase_timestamp) AS month,
        SUM(payment_value) AS revenue
    FROM olist.orders
    JOIN olist.order_payments USING(order_id)
    GROUP BY 1
)
SELECT
    month,
    revenue,
    revenue - LAG(revenue) OVER (ORDER BY month) AS revenue_change
FROM monthly_revenue;

-- RFM Customer Segmentation base (Recency, Frequency, Monetary)
-- Used for targeted retention campaigns to prevent churn.
SELECT 
    c.customer_unique_id,
    MAX(o.order_purchase_timestamp) AS last_purchase,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(p.payment_value) AS total_spent,
    NTILE(4) OVER (ORDER BY SUM(p.payment_value) DESC) as monetary_segment
FROM olist.customers c
JOIN olist.orders o USING(customer_id)
JOIN olist.order_payments p USING(order_id)
GROUP BY 1;

-- =========================================================
-- PHASE 5: POWER BI / DASHBOARD VIEWS
-- Purpose: Flattening the Star Schema into optimized views 
--          to increase Power BI performance & support AI/MCP.
-- =========================================================

-- 1. Orders Enriched View
CREATE OR REPLACE VIEW olist.v_orders_enriched AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  c.customer_unique_id,
  c.customer_zip_code_prefix,
  c.customer_city,
  c.customer_state
FROM olist.orders o
JOIN olist.customers c ON c.customer_id = o.customer_id;

-- 2. Order Items Enriched View
CREATE OR REPLACE VIEW olist.v_order_items_enriched AS
SELECT
  oi.order_id,
  oi.order_item_id,
  oi.product_id,
  oi.seller_id,
  oi.shipping_limit_date,
  oi.price,
  oi.freight_value,
  p.product_category_name,
  p.product_weight_g,
  s.seller_city,
  s.seller_state
FROM olist.order_items oi
LEFT JOIN olist.products p ON p.product_id = oi.product_id
LEFT JOIN olist.sellers s ON s.seller_id = oi.seller_id;

-- 3. Payments Aggregated per Order
CREATE OR REPLACE VIEW olist.v_payments_by_order AS
SELECT
  order_id,
  COUNT(*) AS payment_rows,
  SUM(payment_value) AS total_payment_value
FROM olist.order_payments
GROUP BY order_id;

-- 4. Reviews View
CREATE OR REPLACE VIEW olist.v_reviews AS
SELECT
  order_id,
  review_score,
  review_creation_date,
  review_answer_timestamp
FROM olist.order_reviews;

-- 5. MAIN FACT TABLE FOR BI (One row per order)
-- ROI: Reduces compute overhead in BI tools by pre-calculating delivery times.
CREATE OR REPLACE VIEW olist.v_fact_orders AS
SELECT
  oe.order_id,
  oe.customer_unique_id,
  oe.customer_state,
  oe.order_status,
  oe.order_purchase_timestamp,
  pb.total_payment_value,
  r.review_score,
  CASE
    WHEN oe.order_delivered_customer_date IS NULL THEN NULL
    ELSE (oe.order_delivered_customer_date::date - oe.order_purchase_timestamp::date)
  END AS days_to_deliver,
  CASE
    WHEN oe.order_delivered_customer_date IS NULL OR oe.order_estimated_delivery_date IS NULL THEN NULL
    WHEN oe.order_delivered_customer_date::date <= oe.order_estimated_delivery_date::date THEN 1
    ELSE 0
  END AS delivered_on_time_flag
FROM olist.v_orders_enriched oe
LEFT JOIN olist.v_payments_by_order pb ON pb.order_id = oe.order_id
LEFT JOIN olist.v_reviews r ON r.order_id = oe.order_id;

-- =========================================================
-- PHASE 6: FINAL VERIFICATION
-- Purpose: Validate the views generated properly.
-- =========================================================

-- List all created views
SELECT table_schema, table_name
FROM information_schema.views
WHERE table_schema = 'olist'
ORDER BY table_name;

-- Master Fact Table Sanity Check
SELECT
    COUNT(*) AS total_orders,
    ROUND(AVG(total_payment_value)::numeric, 2) AS avg_order_value,
    ROUND(AVG(days_to_deliver)::numeric, 2) AS avg_delivery_days,
    ROUND(AVG(review_score)::numeric, 2) AS avg_review_score
FROM olist.v_fact_orders;