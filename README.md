### Olist E-Commerce Data Engineering Assignment

End-to-end data engineering pipeline built on Databricks (Serverless + Unity Catalog) using PySpark and SQL across 10 milestones. Processes the Olist Brazilian E-Commerce dataset through a medallion architecture (Bronze → Silver → Gold).

---

#### Prerequisites

- Databricks workspace with Serverless compute enabled
- Unity Catalog enabled with access to `main.default` schema
- 9 Olist CSV files uploaded to a Unity Catalog Volume

#### Setup

##### 1. Upload CSV files

1. In Databricks, go to left sidebar → "Data Ingestion" or "Add Data"
2. Upload all 9 CSV files:
   - olist_orders_dataset.csv
   - olist_order_items_dataset.csv
   - olist_customers_dataset.csv
   - olist_sellers_dataset.csv
   - olist_products_dataset.csv
   - olist_order_payments_dataset.csv
   - olist_order_reviews_dataset.csv
   - olist_geolocation_dataset.csv
   - product_category_name_translation.csv
3. Set destination: Catalog = `main`, Schema = `default`, Volume = `dataengineeringassignment`
4. Files will be at: `/Volumes/main/default/dataengineeringassignment/`

##### 2. Import the notebook

1. Left sidebar → "Workspace" → your user folder
2. Click "Import" and upload `DataEngineeringAssignment.py`
3. Databricks converts it into a notebook

##### 3. Attach compute and run

1. Open the notebook, click the compute dropdown (top-right), select "Serverless"
2. In cell A1.1, verify `BASE_PATH` matches your Volume path:
   ```python
   BASE_PATH = "/Volumes/main/default/dataengineeringassignment/"
   ```
3. Click "Run All" — cells must run top to bottom due to dependencies

#### Dataset

| File | Rows | Description |
|------|------|-------------|
| olist_customers_dataset.csv | 99,441 | Customer master (id, zip, city, state) |
| olist_geolocation_dataset.csv | 1,000,163 | Geolocation lat/lng by zip prefix |
| olist_orders_dataset.csv | 99,441 | Order headers with status and timestamps |
| olist_order_items_dataset.csv | 112,650 | Line items (product, seller, price, freight) |
| olist_order_payments_dataset.csv | 103,886 | Payment methods per order |
| olist_order_reviews_dataset.csv | 99,224 | Review scores and comments |
| olist_products_dataset.csv | 32,951 | Product catalogue with dimensions |
| olist_sellers_dataset.csv | 3,095 | Seller master (id, zip, city, state) |
| product_category_name_translation.csv | 71 | Portuguese to English category mapping |

---

#### Pipeline Architecture

##### Medallion overview

```
  CSV Files (Volumes)          Bronze Layer              Silver Layer              Gold Layer
  ──────────────────          ─────────────             ─────────────             ──────────

  olist_orders               ┌─────────────┐          ┌─────────────┐          ┌──────────────────────┐
  olist_order_items          │ Raw CSVs     │          │ Cleaned &   │          │ Aggregates,          │
  olist_customers            │ loaded with  │──────►   │ Validated   │──────►   │ Features &           │
  olist_sellers              │ inferred +   │          │ Deduplicated│          │ Business Metrics     │
  olist_products             │ explicit     │          │ Standardized│          │                      │
  olist_payments             │ schemas      │          │ Type-safe   │          │                      │
  olist_reviews              └─────────────┘          └─────────────┘          └──────────────────────┘
  olist_geolocation
  product_category_                A1                     A2, A3                  A4, A5, A6, A8
  name_translation
```

##### Bronze layer (A1)

Raw data loaded from CSV files with both inferred and explicit schemas.

| Source File | DataFrame | Temp View | Rows | Columns |
|---|---|---|---|---|
| olist_orders_dataset.csv | df_orders | orders | ~99,441 | 8 |
| olist_order_items_dataset.csv | df_items | items | ~112,650 | 7 |
| olist_customers_dataset.csv | df_customers | customers | ~99,441 | 5 |
| olist_sellers_dataset.csv | df_sellers | sellers | ~3,095 | 4 |
| olist_products_dataset.csv | df_products | products | ~32,951 | 9 |
| olist_order_payments_dataset.csv | df_payments | payments | ~103,886 | 5 |
| olist_order_reviews_dataset.csv | df_reviews | reviews | ~99,224 | 7 |
| olist_geolocation_dataset.csv | df_geo | geo | ~1,000,163 | 5 |
| product_category_name_translation.csv | df_translation | translation | ~71 | 2 |

Additional bronze views: `file_metadata`, `exploratory_summary`

##### Silver layer (A2, A3)

**A2 — Cleansing outputs:**

| DataFrame | Temp View | Transformations |
|---|---|---|
| df_orders_clean | orders_clean | null handling (approved_at imputed), has_missing_dates flag, date_anomaly flag |
| df_items_clean | items_clean | dedup, invalid_price and invalid_freight flags |
| df_customers_clean | customers_clean | city/state standardized (initcap/upper + trim) |
| df_sellers_clean | sellers_clean | city/state standardized (initcap/upper + trim) |
| df_products_clean | products_clean | null category imputed with 'unknown' |
| df_payments_clean | payments_clean | dedup, invalid_payment flag |
| df_reviews_clean | reviews_clean | null comments imputed with empty string, invalid_score flag |
| df_geo_clean | geo_clean | dedup, city/state standardized |
| df_translation_clean | translation_clean | dedup only |

Additional cleansing views: `null_report`, `dup_report`, `cleansing_log`

**A3 — Enrichment outputs:**

| View | Description |
|---|---|
| `dim_geography` | SCD2 geography dimension keyed by zip_prefix, versioned by city/state changes |
| `orders_ts` | Orders with repaired timestamps (forward-filled where discrepancy <= 2 hours) |
| `delivery_data` | Delivered orders joined with items, includes on_time flag |
| `seller_trailing_ontime` | Per order-item: seller's trailing 30-day on-time delivery rate before purchase date |
| `seller_funnel` | Per seller per month: P->A and A->D conversion rates |
| `seller_revenue_share` | Per seller: top 5 categories by revenue + OTHERS with revenue share percentage |

##### Gold layer (A4, A5, A6, A8)

**A4 — Business aggregates:**

| View | Description |
|---|---|
| `delivery_severity` | Delivered orders with late_severity UDF applied (-2 to 2 bucketing) |
| `seller_severity_dist` | Late-severity distribution per seller |
| `orders_state` | Orders joined with customers, includes order_month and customer_state |
| `category_delay_dist` | P50, P90, P99 delivery delay per product category |
| `seller_rolling_ontime` | 7-day rolling on-time rate per seller |
| `payments_pivot` | Payment values pivoted by type per order |

**A5 — Performance optimization outputs:**

| Table / View | Description |
|---|---|
| `main.default.orders_partitioned` | Delta table partitioned by order_status |
| `main.default.orders_delta_zorder` | Delta table Z-ORDERed by customer_id |
| `customer_sessions` | Session count and avg orders per session per customer (7-day gap threshold) |
| `avg_score_state_month` | Average review score by customer_state and month |
| `freight_outliers` | Order items where freight_value exceeds seller's P95 |

**A6 — Visualization base:**

| View | Description |
|---|---|
| `revenue_base` | Unified table: items + orders + customers + products + translation with total_revenue, order_month, delay_days, category |

**A8 — Advanced SQL + PySpark:**

| View | Description |
|---|---|
| `customer_cohort_retention` | Monthly acquisition cohorts with 3-month retention percentages by state |
| `seller_mom_growth` | Seller rolling 30-day revenue, MoM growth %, ranked by state and month |
| `customer_rfm` | Recency, frequency, monetary per customer with deciles and composite RFM score |

##### Governance and orchestration views (A7, A9)

| View | Milestone | Description |
|---|---|---|
| `rbac_plan` | A7 | Role-based access control matrix (5 roles x 3 layers) |
| `data_dictionary` | A7 | Column-level metadata for all 9 tables |
| `data_lineage` | A7 | Source-to-target lineage for bronze, silver, gold tables |
| `rbac_grants` | A7 | Simulated Unity Catalog grant statements per role |
| `pipeline_dag` | A9 | Task dependency graph for the orchestration job |

---

#### Entity Relationship Diagram

```
                                    ┌─────────────────────────┐
                                    │       geolocation        │
                                    │─────────────────────────│
                                    │ geolocation_zip_code_    │
                                    │   prefix (PK, non-unique)│
                                    │ geolocation_lat          │
                                    │ geolocation_lng          │
                                    │ geolocation_city         │
                                    │ geolocation_state        │
                                    └────────┬───────┬────────┘
                                             │       │
                              (zip_code_prefix)     (zip_code_prefix)
                                             │       │
┌──────────────────────┐            ┌────────┴───────┴────────┐
│      reviews         │            │       customers          │
│──────────────────────│            │─────────────────────────│
│ review_id (PK)       │            │ customer_id (PK)         │
│ order_id (FK)        │──┐         │ customer_unique_id       │
│ review_score         │  │         │ customer_zip_code_prefix │
│ review_comment_title │  │         │ customer_city            │
│ review_comment_msg   │  │         │ customer_state           │
│ review_creation_date │  │         └────────────┬────────────┘
│ review_answer_ts     │  │                      │
└──────────────────────┘  │               (customer_id)
                          │                      │
┌──────────────────────┐  │         ┌────────────┴────────────┐
│     payments         │  │         │         orders            │
│──────────────────────│  │         │─────────────────────────│
│ order_id (FK, PK)    │──┤         │ order_id (PK)            │
│ payment_sequential   │  ├─────────│ customer_id (FK)         │
│   (PK)               │  │         │ order_status             │
│ payment_type         │  │         │ order_purchase_timestamp  │
│ payment_installments │  │         │ order_approved_at         │
│ payment_value        │  │         │ order_delivered_carrier   │
└──────────────────────┘  │         │ order_delivered_customer  │
                          │         │ order_estimated_delivery  │
                          │         └────────────┬────────────┘
                          │                      │
                          │               (order_id)
                          │                      │
                          │         ┌────────────┴────────────┐
                          └─────────│      order_items         │
                                    │─────────────────────────│
                                    │ order_id (FK, PK)        │
                                    │ order_item_id (PK)       │
                                    │ product_id (FK)          │
                                    │ seller_id (FK)           │
                                    │ shipping_limit_date      │
                                    │ price                    │
                                    │ freight_value            │
                                    └───┬─────────────────┬───┘
                                        │                 │
                                 (product_id)        (seller_id)
                                        │                 │
                           ┌────────────┴──────┐  ┌──────┴────────────┐
                           │     products      │  │      sellers       │
                           │───────────────────│  │───────────────────│
                           │ product_id (PK)   │  │ seller_id (PK)    │
                           │ product_category_ │  │ seller_zip_code_  │
                           │   name (FK)       │  │   prefix          │
                           │ product_name_     │  │ seller_city       │
                           │   lenght          │  │ seller_state      │
                           │ product_desc_     │  └───────────────────┘
                           │   lenght          │          │
                           │ product_photos_qty│   (zip_code_prefix)
                           │ product_weight_g  │          │
                           │ product_length_cm │          │
                           │ product_height_cm │     to geolocation
                           │ product_width_cm  │
                           └────────┬──────────┘
                                    │
                          (product_category_name)
                                    │
                           ┌────────┴──────────┐
                           │   translation     │
                           │───────────────────│
                           │ product_category_ │
                           │   name (PK)       │
                           │ product_category_ │
                           │   name_english    │
                           └───────────────────┘
```

##### Relationships

| From | To | Join Key | Cardinality |
|---|---|---|---|
| orders | customers | customer_id | N:1 |
| order_items | orders | order_id | N:1 |
| order_items | products | product_id | N:1 |
| order_items | sellers | seller_id | N:1 |
| payments | orders | order_id | N:1 |
| reviews | orders | order_id | N:1 |
| products | translation | product_category_name | N:1 |
| customers | geolocation | zip_code_prefix | N:N |
| sellers | geolocation | zip_code_prefix | N:N |

##### Primary keys

| Table | Primary Key |
|---|---|
| orders | order_id |
| order_items | (order_id, order_item_id) |
| customers | customer_id |
| sellers | seller_id |
| products | product_id |
| payments | (order_id, payment_sequential) |
| reviews | review_id |
| geolocation | geolocation_zip_code_prefix (non-unique) |
| translation | product_category_name |

---

#### Milestone Dependency Flow

##### Execution DAG

```
A1 (Data Ingestion)
 │
 ▼
A2 (Data Cleansing & Validation)
 │
 ▼
A3 (Enrichment & CDC Design)
 │
 ▼
A4 (Business & Advanced Feature Aggregates)
 │
 ▼
A5 (Performance Optimization)
 │
 ├──────────────────────────────┐
 │                              │
 ▼                              ▼
A6 (Visualization)            A8 (Advanced SQL + PySpark)
 │                              │
 ▼                              │
A7 (Governance & Security)     │
 │                              │
 └──────────────┬───────────────┘
                │
                ▼
A9 (Orchestration & Automation)
 │
 ▼
A10 (Documentation & Code Quality)
```

A6/A7 and A8 both depend on A5 but are independent of each other.

##### Key view dependencies

```
orders ──► orders_ts ──► delivery_data ──► delivery_severity
           (A3)          (A3)               (A4)
                              │                  ├──► seller_severity_dist
                              │                  ├──► category_delay_dist
                              │                  └──► seller_rolling_ontime
                              │
                              ├──► seller_trailing_ontime (A3)
                              └──► seller_funnel (A3)

orders_ts + customers ──► orders_state (A4)
orders_ts + customers ──► customer_cohort_retention (A8)
orders_ts ──► customer_sessions (A5)

items + orders_ts + customers + products + translation ──► revenue_base (A6)

items + seller P95 ──► freight_outliers (A5)
reviews + orders_ts + customers ──► avg_score_state_month (A5)
items + orders_ts + sellers ──► seller_mom_growth (A8)
orders_ts + items ──► customer_rfm (A8)
payments ──► payments_pivot (A4)
geolocation ──► dim_geography (A3)
```

---

#### Visualization Configuration (A6)

After running A6 cells, configure charts in the Databricks UI:

| Cell | Chart Type | X Axis / Key | Y Axis / Value |
|---|---|---|---|
| A6.1 | Counter | — | total_revenue |
| A6.2 | Bar | category_revenue | category |
| A6.3 | Line | order_month | monthly_revenue |
| A6.4 | Pie | payment_type | total_payment |
| A6.5 | Box | category | delay_days |

---

#### Orchestration Job (A9)

After running A9.1:
1. Left sidebar → "Workflows"
2. Find "Olist_DEA_Pipeline" in the job list
3. The job is created but not triggered - run it manually from the Workflows page

To build a dashboard:
1. Left sidebar → "Dashboards" → "Create Dashboard"
2. Add each visualization as a widget
3. Optionally add filters for customer_state and date range

---

#### Milestone Index

| Milestone | Description |
|---|---|
| A1 | Loads 9 CSVs with inferred and explicit schemas, creates metadata and summary tables |
| A2 | Identifies/handles nulls, removes duplicates, standardizes types/formats, validates ranges, logs all actions |
| A3 | Builds SCD2 geography, repairs timestamps, computes trailing on-time rates, funnel conversions, revenue shares |
| A4 | Creates late-severity UDF, pivots order status by state/month, computes delay percentiles, rolling on-time, payment pivot |
| A5 | Demonstrates broadcast join, Z-ORDER vs partitioning, computes sessions, deduped review scores, freight outliers |
| A6 | Prepares data for KPI card, bar chart, line chart, pie chart, box plot |
| A7 | Documents RBAC plan, data dictionary, data lineage, simulates grants |
| A8 | Builds cohort retention by state, seller MoM growth with ranking, customer RFM scoring with deciles |
| A9 | Creates Databricks Job via SDK with task DAG, demonstrates real failure/recovery with retry logic |
| A10 | Architecture diagrams, ER diagram, dependency flow, validation cell, user instructions |

---

#### Troubleshooting

| Issue | Solution |
|---|---|
| `Path does not exist` | Verify BASE_PATH matches your Volume path |
| `DBFS_DISABLED` | Use Unity Catalog Volumes (`/Volumes/...`) instead of DBFS paths |
| `getDbUtils` error | Serverless doesn't support this method — use `WorkspaceClient()` from databricks SDK |
| Slow first cell | Normal on Serverless — cold start takes 1-2 minutes |
| Z-ORDER fails | Use `saveAsTable("main.default.table_name")` instead of `.save("/tmp/...")` |
| Pie chart shows multiple pies | Remove extra value columns — keep only payment_type and total_payment |
| Job creation fails | Check if Workflows is available. If permissions error, create the job manually via UI |
