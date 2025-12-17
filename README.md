“This repo is for portfolio demonstration only. No commercial use permitted.”
# Unified POS Data Platform

## 1. Overview

This project builds a **unified analytical data platform** for three different
Point-of-Sale (POS) systems:

- **POS A** – two separate databases (one per location)
- **POS B** – single multi-tenant database (multiple locations)
- **POS C** – three separate databases (one per location)

The goal is to:

1. **Normalize and unify schemas** from POS A/B/C into a single star schema.
2. Provide **reliable answers** to three core business questions:
   - Revenue by location
   - Service popularity (overall and by location)
   - Customer engagement by membership tier
3. Design the system to **generalize to a new POS D** with an unknown schema.
4. Implement:
   - An **ETL pipeline** (Python + SQLite locally, cloud-ready)
   - A **web API + simple frontend** (FastAPI)
   - A **natural language query interface** (rule-based, LLM-ready)
   - A **cloud architecture** for scaling to millions of records
   - A **unified schema declaration** (`unified_schema.json`) for documentation & automation.

---

## 2. Data Layout & Discovery

### 2.1. Disk layout

The project expects a folder like:

build/
  unify_pos.py
  api.py
  data/
    posa/
      db1/*.csv
      db2/*.csv
    posb/
      db1/*.csv
    posc/
      db1/*.csv
      db2/*.csv
      db3/*.csv
    posd/
      db1/
        locations_*.csv
        customers_*.csv
        service_catalog_*.csv
        sales_*.csv
        memberships_*.csv   # optional


---

## 3. Business Metrics

### 3.1. Endpoints

The project has multiple endpoints that draw some key conclusions in regards to the business:

  “Core metric endpoints” – list /revenue_by_location, /service_popularity, /customer_engagement_by_membership, etc.

  “Analysis endpoints” – list /analysis/revenue_by_location, /analysis/top_services, /analysis/membership_impact and mention they return results, methodology, assumptions, and sql used.
