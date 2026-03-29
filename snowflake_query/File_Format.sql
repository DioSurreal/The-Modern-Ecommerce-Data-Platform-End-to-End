USE ROLE ACCOUNTADMIN;
USE DATABASE ECOMMERCE_RAW;
CREATE OR REPLACE SCHEMA LANDING;

GRANT USAGE ON INTEGRATION S3_INTEGRATION TO ROLE ACCOUNTADMIN;

CREATE OR REPLACE STAGE ECOMMERCE_RAW.STAGING.MY_S3_STAGE
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = 's3://my-ecommerce-project-landing-zone/';

-- สร้างตัวแปลภาษา CSV (เพราะไฟล์เรามี Header และใช้เครื่องหมายคอมม่า)
CREATE OR REPLACE FILE FORMAT LANDING.CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;

  -- 1. Distribution Centers
CREATE OR REPLACE TABLE LANDING.DISTRIBUTION_CENTERS (
    id INTEGER, name STRING, latitude FLOAT, longitude FLOAT
);

-- 2. Events (ตัวเอกของ Clickstream)
CREATE OR REPLACE TABLE LANDING.EVENTS (
    id INTEGER, user_id FLOAT, sequence_number INTEGER, session_id STRING,
    created_at STRING, ip_address STRING, city STRING, state STRING,
    postal_code STRING, browser STRING, traffic_source STRING, uri STRING, event_type STRING
);

-- 3. Inventory Items
CREATE OR REPLACE TABLE LANDING.INVENTORY_ITEMS (
    id INTEGER, product_id INTEGER, created_at STRING, sold_at STRING,
    cost FLOAT, product_category STRING, product_name STRING, product_brand STRING,
    product_retail_price FLOAT, product_department STRING, product_sku STRING,
    product_distribution_center_id INTEGER
);

-- 4. Order Items
CREATE OR REPLACE TABLE LANDING.ORDER_ITEMS (
    id INTEGER, order_id INTEGER, user_id INTEGER, product_id INTEGER,
    inventory_item_id INTEGER, status STRING, created_at STRING,
    shipped_at STRING, delivered_at STRING, returned_at STRING, sale_price FLOAT
);

-- 5. Orders
CREATE OR REPLACE TABLE LANDING.ORDERS (
    order_id INTEGER, user_id INTEGER, status STRING, gender STRING,
    created_at STRING, returned_at STRING, shipped_at STRING, 
    delivered_at STRING, num_of_item INTEGER
);

-- 6. Products
CREATE OR REPLACE TABLE LANDING.PRODUCTS (
    id INTEGER, cost FLOAT, category STRING, name STRING,
    brand STRING, retail_price FLOAT, department STRING, 
    sku STRING, distribution_center_id INTEGER
);

-- 7. Users
CREATE OR REPLACE TABLE LANDING.USERS (
    id INTEGER, first_name STRING, last_name STRING, email STRING,
    age INTEGER, gender STRING, state STRING, street_address STRING,
    postal_code STRING, city STRING, country STRING, latitude FLOAT,
    longitude FLOAT, traffic_source STRING, created_at STRING
);

COPY INTO LANDING.DISTRIBUTION_CENTERS
FROM @STAGING.MY_S3_STAGE/raw/distribution_centers.csv
FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO LANDING.EVENTS
FROM @STAGING.MY_S3_STAGE/raw/events.csv
FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO LANDING.INVENTORY_ITEMS
FROM @STAGING.MY_S3_STAGE/raw/inventory_items.csv
FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO LANDING.ORDER_ITEMS
FROM @STAGING.MY_S3_STAGE/raw/order_items.csv
FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO LANDING.ORDERS
FROM @STAGING.MY_S3_STAGE/raw/orders.csv
FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO LANDING.PRODUCTS
FROM @STAGING.MY_S3_STAGE/raw/products.csv
FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO LANDING.USERS
FROM @STAGING.MY_S3_STAGE/raw/users.csv
FILE_FORMAT = (FORMAT_NAME = 'LANDING.CSV_FORMAT')
ON_ERROR = 'CONTINUE';
