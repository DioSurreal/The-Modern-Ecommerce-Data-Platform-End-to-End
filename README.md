# The Modern Ecommerce Data Platform End-to-End

This project builds a modern data platform for ecommerce, covering the full path from ingestion to transformation to analytics-ready data marts.

## Overview of the 3 Core Features

### F1: Real-time User Sessionization and Journey
![F1](images\F1_click_stream_session_lineage.jpg)
**Objective**
- Process clickstream data continuously
- Automatically group user behavior into sessions
- Produce journey-ready data for customer behavior analysis

**What the system does**
- `event_maker/s3_stream_producer.py` reads `events.csv` and uploads each event to S3 as individual JSON files
- `snowflake_query/click_stream_consumer_s3.sql` creates `STAGE` + `PIPE (AUTO_INGEST=TRUE)` for automatic ingestion into Snowflake
- `stg_clickstream.sql` parses JSON (`VARIANT`) into usable columns such as `event_ts`, `event_action`, and `page_url`
- `int_sessionized_events.sql` performs sessionization with a 30-minute inactivity rule
- `fct_clickstream_sessions.sql` joins sessionized events with user data for customer journey analysis

**Core logic in code (F1)**
- Use `lag(event_ts) over (partition by raw_session_id order by event_ts)` to find the previous event
- If time gap > 1,800 seconds, mark as a new session
- Build `enhanced_session_id` using a running sum of the new-session flag

---

### F2: Customer 360 and Predictive CLV
![F2_1](images\F2_daily_sales.png)
![F2_2](images\F2_revenue_performance.png)
**Objective**
- Combine users, orders, and order items
- Build customer-level profiles and CLV-oriented metrics

**What the system does**
- Loads batch CSV data from S3 into Snowflake using `COPY INTO` (`snowflake_query/File_Format.sql`)
- Uses dbt staging models: `stg_users.sql`, `stg_orders.sql`, `stg_order_items.sql`
- Builds `int_customer_orders.sql` for customer-level aggregation
- Publishes marts such as `fct_revenue_performance.sql` and `fct_daily_sales.sql`

**Core logic in code (F2)**
- `total_orders = count(distinct order_id)`
- `lifetime_revenue = sum(sale_price)` excluding `Cancelled` and `Returned`
- `average_order_value = avg(sale_price)`
- Track `first_order_ts` and `last_order_ts`
- Customer segmentation:
  - `High Value` for revenue > 500
  - `Medium Value` for revenue between 100 and 500
  - `Low Value` for revenue > 0
  - `Lead` for revenue = 0

**Important note**
- Current “Predictive CLV” is rule-based/proxy segmentation from historical spend
- A full ML-based CLV prediction model is not implemented yet

---

### F3: Inventory Intelligence and Logistics Optimization
![F3](images\F3_geo_logistics_order_items.png)
**Objective**
- Combine products, inventory, distribution centers, and customer location
- Analyze distance, shipping cost, and net margin to optimize logistics decisions

**What the system does**
- Uses dbt staging models: `stg_products.sql`, `stg_inventory_items.sql`, `stg_distribution_centers.sql`
- Joins logistics-related data in `int_order_logistics_joined.sql`
- Computes logistics cost and margin in `fct_logistics_optimization.sql`

**Core logic in code (F3)**
- Distance: `st_distance(st_makepoint(user_lng,user_lat), st_makepoint(dc_lng,dc_lat))/1000 as distance_km`
- Estimated shipping cost: `estimated_shipping_cost = round(2 + distance_km * 0.05, 2)`
- Net margin per item: `net_profit_margin = sale_price - product_cost - estimated_shipping_cost`

**Important note**
- The project has enough base data to extend into Stock Turnover analytics
- A dedicated Stock Turnover model is not yet implemented

## End-to-End Data Flow

1. Upload raw dataset files to S3 (`looker-ecommerce-dataset/upload_to_s3.py`)
2. Simulate clickstream and stream events to S3 (`event_maker/s3_stream_producer.py`)
3. Snowflake ingests data via two paths
   - Batch CSV via `COPY INTO`
   - Streaming JSON via Snowpipe (`AUTO_INGEST`)
4. dbt transforms data through layered modeling
   - Staging -> Intermediate -> Marts
5. Final tables are ready for F1/F2/F3 analytics use cases

## Tech Stack and Why It Is Used

### 1) AWS IAM (Cloud Security)
- Enforces least-privilege access to cloud resources
- Works with IAM roles/policies and Snowflake storage integration
- In this project: secures Snowflake/Snowpipe access to S3

### 2) AWS S3 (Data Lake Landing Zone)
- Stores both batch and streaming source data
- In this project: entry point for CSV files and clickstream JSON files before Snowflake ingestion

### 3) Snowpipe (Event-Driven Ingestion)
- Detects new files in S3 and ingests them into Snowflake automatically
- In this project: enables near real-time clickstream ingestion for F1

### 4) Snowflake (Cloud Data Warehouse)
- Central platform for storing and processing analytical data
- Uses `STAGE`, `FILE FORMAT`, `COPY INTO`, `PIPE`, `VARIANT`, and geospatial SQL functions
- In this project: supports both batch and streaming workloads on one platform

### 5) dbt Core (Transformation Engine)
- Organizes SQL transformation into reusable, modular data models
- Uses `ref()` lineage and Jinja/macros for maintainable development
- Includes `dbt_expectations` package for data quality workflows
- In this project: builds staging, intermediate, and marts for F1/F2/F3

### 6) SQL Analytical Logic (Advanced Analytics)
- Uses window functions and analytical SQL as core business logic
- In this project:
  - F1: sessionization logic
  - F2: customer value profiling and segmentation
  - F3: geospatial distance, shipping cost estimation, and net margin

### 7) Terraform (Infrastructure as Code)
- Provisions AWS and Snowflake resources from code
- Covers S3 bucket, IAM role/policy, Snowflake warehouse/database/role/storage integration
- In this project: improves reproducibility and reduces manual setup drift

### 8) Python (`boto3`, `pandas`, `python-dotenv`)
- Automates ingestion and event simulation scripts
- In this project:
  - `looker-ecommerce-dataset/upload_to_s3.py`
  - `event_maker/s3_stream_producer.py`
- Simplifies repeatable data onboarding and stream simulation

### 9) Docker (Runtime Base)
- Standardizes runtime dependencies and execution environment
- In this project: helps keep execution consistent across machines

## Current Deliverables

- F1: `DBT_ECOMMERCE.FCT_CLICKSTREAM_SESSIONS`
- F2: `DBT_ECOMMERCE.INT_CUSTOMER_ORDERS`, `DBT_ECOMMERCE.DIM_CUSTOMERS`, `DBT_ECOMMERCE.FCT_REVENUE_PERFORMANCE`, `DBT_ECOMMERCE.FCT_DAILY_SALES`
- F3: `DBT_ECOMMERCE.FCT_LOGISTICS_OPTIMIZATION`
