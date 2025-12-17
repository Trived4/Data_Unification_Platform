from fastapi import FastAPI, Query, Response, HTTPException
from typing import List, Dict, Optional, Any
from fastapi.responses import HTMLResponse
import sqlite3
import os


# -----------------------------------------------------
# App & DB helpers
# -----------------------------------------------------

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "unified.db")


def get_conn():
    return sqlite3.connect(DB_PATH)


def run_sql_dicts(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """
    Helper: run SQL and return a list of dicts instead of tuples.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def run_sql_dicts(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """
    Helper: run SQL and return a list of dicts instead of tuples.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# -----------------------------------------------------
# Basic utility / debug endpoints
# -----------------------------------------------------

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Avoid noisy 404s for /favicon.ico in the browser"""
    return Response(status_code=204)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/debug_row_counts")
def debug_row_counts():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    tables = [
        "dim_location",
        "dim_customer",
        "dim_service",
        "fact_transaction_header",
        "fact_transaction_line_item",
    ]
    result = {}
    for t in tables:
        cnt = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        result[t] = cnt
    conn.close()
    return result

@app.get("/schema_diff")
def schema_diff(
    pos_system: Optional[str] = Query(
        None,
        description="Filter by POS system (e.g., POSA, POSB, POSC, POSD)",
    ),
    source_table: Optional[str] = Query(
        None,
        description="Filter by source table name (e.g., posa.customers, posb.sales)",
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of grouped rows to return",
    ),
):
    """
    Show unmapped / unexpected columns discovered during ETL.

    Data comes from stg_unmapped_columns, which is populated by unify_pos.py
    when it finds extra columns that are not used by the canonical model.

    You can filter by pos_system and/or source_table.
    """
    # Normalize casing for matching
    if pos_system:
        pos_system = pos_system.upper().strip()

    if source_table:
        source_table = source_table.lower().strip()
    
    # Build WHERE clause dynamically
    clauses = []
    params: list[Any] = []

    if pos_system:
        clauses.append("UPPER(pos_system) = ?")
        params.append(pos_system)

    if source_table:
        clauses.append("LOWER(source_table) = ?")
        params.append(source_table)

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
    SELECT
        pos_system,
        source_table,
        column_name,
        inferred_type,
        COUNT(*)           AS occurrences,
        MAX(detected_at)   AS last_seen
    FROM stg_unmapped_columns
    {where_sql}
    GROUP BY pos_system, source_table, column_name, inferred_type
    ORDER BY last_seen DESC
    LIMIT ?
    """
    params.append(limit)

    try:
        rows = run_sql_dicts(sql, params=tuple(params))
    except sqlite3.OperationalError as e:
        # Most likely the ETL hasn't been re-run with the new DDL
        raise HTTPException(
            status_code=500,
            detail=(
                "stg_unmapped_columns table not found. "
                "Re-run unify_pos.py after updating the ETL with schema diagnostics."
            ),
        ) from e

    return {
        "metric": "schema_diff",
        "filters": {
            "pos_system": pos_system,
            "source_table": source_table,
            "limit": limit,
        },
        "results": rows,
    }

@app.get("/posd_mapping")
def posd_mapping(
    db_name: Optional[str] = Query(
        None,
        description="Optional filter for DB name (e.g., db1, DB1, Db2)"
    ),
    source_table: Optional[str] = Query(
        None,
        description="Optional filter for source table name (e.g., customers, transactions)"
    ),
    min_confidence: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="Minimum confidence threshold"
    ),
    limit: int = Query(
        200,
        ge=1,
        le=1000,
        description="Maximum rows to return"
    ),
):
    """
    Return POSD auto-mapping results.
    Supports:
      - case-insensitive filters
      - substring table matching ("transactions" → "posd_db2_transactions")
    """

    clauses = []
    params: list[Any] = []

    # -------------------------
    # CASE-INSENSITIVE DB NAME
    # -------------------------
    if db_name:
        clauses.append("UPPER(db_name) = UPPER(?)")
        params.append(db_name)

    # --------------------------------------------------
    # CASE-INSENSITIVE PARTIAL MATCH FOR SOURCE TABLE
    # --------------------------------------------------
    if source_table:
        clauses.append("LOWER(source_table) LIKE ?")
        params.append(f"%{source_table.lower()}%")

    # confidence filter
    clauses.append("confidence >= ?")
    params.append(min_confidence)

    # Build WHERE SQL safely
    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    # -------------------------
    # FINAL SQL
    # -------------------------
    sql = f"""
        SELECT
            pos_system,
            db_name,
            source_table,
            source_column,
            target_table,
            target_column,
            confidence,
            sample_value,
            detected_at
        FROM stg_posd_mapping
        {where_sql}
        ORDER BY confidence DESC, detected_at DESC
        LIMIT ?
    """

    params.append(limit)

    # execute
    rows = run_sql_dicts(sql, params=tuple(params))

    return {
        "metric": "posd_mapping",
        "filters": {
            "db_name": db_name,
            "source_table": source_table,
            "min_confidence": min_confidence,
            "limit": limit
        },
        "results": rows,
    }

@app.get("/service_popularity")
def service_popularity(by_location: bool = Query(False)):
    """
    Popularity of services, optionally broken out by location.
    """
    conn = get_conn()
    cur = conn.cursor()
    if by_location:
        cur.execute(
            """
            SELECT
                dl.location_code,
                ds.service_name,
                SUM(fli.quantity) as total_quantity
            FROM fact_transaction_line_item fli
            JOIN fact_transaction_header fh
              ON fli.transaction_header_sk = fh.transaction_header_sk
            JOIN dim_location dl
              ON fh.location_sk = dl.location_sk
            JOIN dim_service ds
              ON fli.service_sk = ds.service_sk
            GROUP BY dl.location_code, ds.service_name
            ORDER BY dl.location_code, total_quantity DESC
            """
        )
        rows = cur.fetchall()
        conn.close()
        return [
            {
                "location_code": r[0],
                "service_name": r[1],
                "total_quantity": r[2],
            }
            for r in rows
        ]
    else:
        cur.execute(
            """
            SELECT
                ds.service_name,
                SUM(fli.quantity) as total_quantity
            FROM fact_transaction_line_item fli
            JOIN dim_service ds
              ON fli.service_sk = ds.service_sk
            GROUP BY ds.service_name
            ORDER BY total_quantity DESC
            """
        )
        rows = cur.fetchall()
        conn.close()
        return [
            {"service_name": r[0], "total_quantity": r[1]}
            for r in rows
        ]


@app.get("/top_services_by_location")
def top_services_by_location(top_n: int = 5):
    """
    For each location, return the top N services by quantity sold.
    """
    sql = """
    WITH svc_agg AS (
        SELECT
            d.location_name,
            s.service_name,
            SUM(li.quantity)     AS total_quantity,
            SUM(li.line_amount)  AS total_revenue
        FROM fact_transaction_line_item li
        JOIN fact_transaction_header h
          ON li.transaction_header_sk = h.transaction_header_sk
        JOIN dim_location d
          ON h.location_sk = d.location_sk
        JOIN dim_service s
          ON li.service_sk = s.service_sk
        GROUP BY d.location_name, s.service_name
    ),
    ranked AS (
        SELECT
            location_name,
            service_name,
            total_quantity,
            total_revenue,
            ROW_NUMBER() OVER (
                PARTITION BY location_name
                ORDER BY total_quantity DESC
            ) AS rn
        FROM svc_agg
    )
    SELECT
        location_name,
        service_name,
        total_quantity,
        total_revenue,
        rn AS rank_within_location
    FROM ranked
    WHERE rn <= ?
    ORDER BY location_name, rn;
    """
    rows = run_sql_dicts(sql, params=(top_n,))
    return {
        "metric": "top_services_by_location",
        "top_n": top_n,
        "results": rows,
    }


@app.get("/customer_engagement_by_membership")
def customer_engagement_by_membership():
    """
    Basic engagement metrics per membership tier.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            dmt.tier_name,
            COUNT(*) as txn_count,
            COUNT(DISTINCT fh.customer_sk) as distinct_customers,
            AVG(fh.total_amount) as avg_transaction_value
        FROM fact_transaction_header fh
        JOIN dim_membership_tier dmt
          ON fh.membership_tier_sk = dmt.membership_tier_sk
        WHERE fh.customer_sk IS NOT NULL
        GROUP BY dmt.tier_name
        ORDER BY txn_count DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "membership_tier": r[0],
            "transaction_count": r[1],
            "distinct_customers": r[2],
            "avg_transaction_value": r[3],
        }
        for r in rows
    ]


@app.get("/customer_frequency_by_membership")
def customer_frequency_by_membership():
    """
    How often customers visit, broken down by membership tier.
    """
    sql = """
    SELECT
        COALESCE(mt.tier_name, 'None')                       AS membership_tier,
        COUNT(DISTINCT h.customer_sk)                        AS unique_customers,
        COUNT(*)                                             AS transactions,
        CAST(COUNT(*) AS REAL) /
          NULLIF(COUNT(DISTINCT h.customer_sk), 0)           AS avg_visits_per_customer
    FROM fact_transaction_header h
    LEFT JOIN dim_membership_tier mt
      ON h.membership_tier_sk = mt.membership_tier_sk
    GROUP BY membership_tier
    ORDER BY transactions DESC;
    """
    rows = run_sql_dicts(sql)
    return {
        "metric": "customer_frequency_by_membership",
        "results": rows,
    }


@app.get("/membership_service_mix")
def membership_service_mix():
    """
    Spend and quantity by service, broken down by membership tier.
    """
    sql = """
    SELECT
        COALESCE(mt.tier_name, 'None')                       AS membership_tier,
        s.service_name,
        SUM(li.quantity)                                     AS total_quantity,
        SUM(li.line_amount)                                  AS total_spend
    FROM fact_transaction_line_item li
    JOIN fact_transaction_header h
      ON li.transaction_header_sk = h.transaction_header_sk
    JOIN dim_service s
      ON li.service_sk = s.service_sk
    LEFT JOIN dim_membership_tier mt
      ON h.membership_tier_sk = mt.membership_tier_sk
    GROUP BY membership_tier, s.service_name
    ORDER BY membership_tier, total_spend DESC;
    """
    rows = run_sql_dicts(sql)
    return {
        "metric": "membership_service_mix",
        "results": rows,
    }


# -----------------------------------------------------
# Composite summary endpoint
# -----------------------------------------------------

@app.get("/summary_all")
def summary_all():
    """
    Returns all key analyses at once:
    - metrics: raw metric outputs
    - analysis: richer business-question style outputs
    """
    metrics = {
        "revenue_by_location": revenue_by_location(),  # simple metric endpoint OK
        "service_popularity_overall": service_popularity(by_location=False),
        "customer_engagement_by_membership": customer_engagement_by_membership(),
        "customer_frequency_by_membership": customer_frequency_by_membership(),
        "membership_service_mix": membership_service_mix(),
    }

    analysis = {
        "revenue_by_location": _compute_revenue_by_location(top_n=3),
        "top_services": analysis_top_services(top_n=5),
        "membership_impact": analysis_membership_impact(),
    }

    return {
        "metrics": metrics,
        "analysis": analysis,
    }


# -----------------------------------------------------
# NL query endpoint
# -----------------------------------------------------

SUPPORTED_TOPICS = [
    "highest revenue location",
    "revenue by location",
    "top services by location",
    "most popular services overall",
    "membership visit frequency",
    "membership service mix",
]


@app.get("/nl_query")
def nl_query(
    q: str = Query(..., min_length=3, description="Natural language question"),
    all: bool = False,
):
    text = q.lower().strip()

    # if user asks for "everything"
    if all:
        return {
            "type": "combined",
            "data": summary_all(),
        }

    text = q.lower().strip()

    # 1) Highest revenue location
    if "highest" in text and "revenue" in text and "location" in text:
        sql = """
        WITH dedup_tx AS (
            SELECT DISTINCT
                   pos_system,
                   pos_transaction_id,
                   location_sk,
                   total_amount
            FROM fact_transaction_header
        )
        SELECT d.location_name,
               SUM(f.total_amount) AS revenue
        FROM dedup_tx f
        JOIN dim_location d ON f.location_sk = d.location_sk
        GROUP BY d.location_name
        ORDER BY revenue DESC
        LIMIT 1;
        """
        rows = run_sql_dicts(sql)
        if not rows:
            return {"answer": "No data available.", "sql": sql, "rows": []}
        r = rows[0]
        return {
            "intent": "highest_revenue_location",
            "answer": f"{r['location_name']} has the highest revenue: {r['revenue']:.2f}.",
            "sql": sql,
            "rows": rows,
        }

    # 2) Revenue by location (full table)
    if "revenue" in text and "location" in text:
        sql = """
        WITH dedup_tx AS (
            SELECT DISTINCT
                   pos_system,
                   pos_transaction_id,
                   location_sk,
                   customer_sk,
                   total_amount
            FROM fact_transaction_header
        )
        SELECT
            d.location_name,
            d.location_code,
            SUM(f.total_amount)                    AS total_revenue,
            COUNT(DISTINCT f.customer_sk)          AS unique_customers,
            CASE WHEN COUNT(DISTINCT f.customer_sk) = 0
                 THEN NULL
                 ELSE SUM(f.total_amount) * 1.0 / COUNT(DISTINCT f.customer_sk)
            END                                    AS revenue_per_customer
        FROM dedup_tx f
        JOIN dim_location d ON f.location_sk = d.location_sk
        GROUP BY d.location_name, d.location_code
        ORDER BY revenue_per_customer DESC;
        """
        rows = run_sql_dicts(sql)
        return {
            "intent": "revenue_by_location",
            "answer": "Revenue by location (including revenue per customer)",
            "sql": sql,
            "rows": rows,
        }

    # 3) Most popular services overall
    if ("popular" in text or "top" in text) and "service" in text:
        sql = """
        SELECT s.service_name,
               SUM(li.quantity) AS total_quantity
        FROM fact_transaction_line_item li
        JOIN dim_service s ON li.service_sk = s.service_sk
        GROUP BY s.service_name
        ORDER BY total_quantity DESC
        LIMIT 5;
        """
        rows = run_sql_dicts(sql)
        return {
            "intent": "top_services_overall",
            "answer": "Top services by quantity sold",
            "sql": sql,
            "rows": rows,
        }

    # 4) Membership frequency
    if "membership" in text and ("visit" in text or "frequency" in text or "tier" in text):
        sql = """
        SELECT
            COALESCE(mt.tier_name, 'None')                       AS membership_tier,
            COUNT(DISTINCT h.customer_sk)                        AS unique_customers,
            COUNT(*)                                             AS transactions,
            CAST(COUNT(*) AS REAL) /
              NULLIF(COUNT(DISTINCT h.customer_sk), 0)           AS avg_visits_per_customer
        FROM fact_transaction_header h
        LEFT JOIN dim_membership_tier mt
          ON h.membership_tier_sk = mt.membership_tier_sk
        GROUP BY membership_tier
        ORDER BY transactions DESC;
        """
        rows = run_sql_dicts(sql)
        return {
            "intent": "membership_frequency",
            "answer": "Average visits per customer by membership tier",
            "sql": sql,
            "rows": rows,
        }

    # Fallback: tell user what we CAN do
    return {
        "intent": "unsupported_question",
        "error": "I don't understand that question yet.",
        "message": (
            "I can currently answer questions about: "
            + ", ".join(SUPPORTED_TOPICS)
        ),
        "supported_topics": SUPPORTED_TOPICS,
    }


# -----------------------------------------------------
# Analysis endpoints (for README / docs)
# -----------------------------------------------------

def _compute_revenue_by_location(top_n: Optional[int] = None) -> Dict[str, Any]:
    """
    Core logic for 'revenue by location' used by both the API endpoint
    and internal callers (summary_all, nl_query when all=true).
    """
    sql = """
    WITH dedup_tx AS (
        SELECT DISTINCT
               pos_system,
               pos_transaction_id,
               location_sk,
               customer_sk,
               total_amount
        FROM fact_transaction_header
    )
    SELECT
        d.location_name,
        SUM(f.total_amount) AS total_revenue
    FROM dedup_tx f
    JOIN dim_location d ON f.location_sk = d.location_sk
    GROUP BY d.location_name
    ORDER BY total_revenue DESC
    """

    params: tuple = ()
    if top_n is not None:
        sql += "\nLIMIT ?"
        params = (int(top_n),)

    rows = run_sql_dicts(sql, params)

    methodology = [
        "Use fact_transaction_header as the atomic transaction table.",
        "Deduplicate on (pos_system, pos_transaction_id) to avoid double-counting retries.",
        "Aggregate total_amount by location_sk and join to dim_location to get location_name.",
        "Include all POS systems (POSA, POSB, POSC) in a unified view.",
    ]

    assumptions = [
        "All monetary amounts are in USD in the unified model.",
        "Transactions with non-positive total_amount are assumed to be refunds/voids and can be excluded in a stricter version.",
        "Location mappings from each POS source to dim_location are correct.",
        "Transactions missing a valid location_sk are excluded; they represent a very small fraction of the data.",
    ]

    return {
        "question": "Business Question 1: Revenue by Location",
        "results": rows,
        "top_n": top_n,
        "methodology": methodology,
        "assumptions": assumptions,
    }

@app.get("/analysis/revenue_by_location")
def analysis_revenue_by_location(
    top_n: Optional[int] = Query(
        None,
        ge=1,
        le=100,
        description="Limit to top N locations (leave blank for all).",
    ),
):
    """
    Business Question 1: Revenue by Location
    Returns results + methodology + assumptions.
    """
    return _compute_revenue_by_location(top_n)


@app.get("/analysis/top_services")
def analysis_top_services(top_n: int = 5):
    """
    Business Question 2: Top Services Overall
    Returns top N services by quantity (and revenue)
    """
    sql = """
    WITH dedup_tx AS (
        -- Deduplicate headers on (pos_system, pos_transaction_id)
        SELECT DISTINCT
               pos_system,
               pos_transaction_id,
               transaction_header_sk
        FROM fact_transaction_header
    ),
    svc_agg AS (
        SELECT
            s.service_name,
            SUM(li.quantity)    AS total_quantity,
            SUM(li.line_amount) AS total_revenue
        FROM fact_transaction_line_item li
        JOIN dedup_tx h
          ON li.transaction_header_sk = h.transaction_header_sk
        JOIN dim_service s
          ON li.service_sk = s.service_sk
        GROUP BY s.service_name
    )
    SELECT
        service_name,
        total_quantity,
        total_revenue
    FROM svc_agg
    ORDER BY total_quantity DESC
    LIMIT ?;
    """
    rows = run_sql_dicts(sql, params=(top_n,))

    methodology = [
        "Use fact_transaction_header + fact_transaction_line_item as the sales records.",
        "Deduplicate headers on (pos_system, pos_transaction_id) to avoid counting the same sale twice.",
        "Join line items to dim_service to attribute quantity and revenue to each service.",
        "Aggregate quantity and revenue by service_name, then sort descending by total_quantity.",
        f"Return the top {top_n} services for quick ranking."
    ]

    assumptions = [
        "Each line item represents a single service sold; bundles or combos are pre-split in the source POS data.",
        "Revenue for a service is the sum of line_amount; discounts are already reflected in the line totals.",
        "Inactive services are still included historically, since they contributed to past revenue.",
        "Null or unknown service IDs are excluded; they are expected to be a negligible share of the data."
    ]

    return {
        "question": "Business Question 2: Top Services Overall",
        "top_n": top_n,
        "results": rows,
        "methodology": methodology,
        "assumptions": assumptions,
    }


@app.get("/analysis/membership_impact")
def analysis_membership_impact():
    """
    Business Question 3: Membership Impact on Visits and Spend
    Compares members vs non-members (and between tiers) on frequency and revenue.
    """
    sql = """
    WITH dedup_tx AS (
        -- Deduplicate on (pos_system, pos_transaction_id) so retries don't over-count
        SELECT DISTINCT
               pos_system,
               pos_transaction_id,
               customer_sk,
               membership_tier_sk,
               total_amount
        FROM fact_transaction_header
    ),
    tier_agg AS (
        SELECT
            membership_tier_sk,
            COUNT(*)                                AS transactions,
            COUNT(DISTINCT customer_sk)             AS unique_customers,
            SUM(total_amount)                       AS total_revenue
        FROM dedup_tx
        GROUP BY membership_tier_sk
    )
    SELECT
        COALESCE(mt.tier_name, 'None')             AS membership_tier,
        ta.transactions,
        ta.unique_customers,
        ta.total_revenue,
        CASE WHEN ta.unique_customers = 0
             THEN NULL
             ELSE CAST(ta.transactions AS REAL) / ta.unique_customers
        END                                        AS avg_visits_per_customer,
        CASE WHEN ta.unique_customers = 0
             THEN NULL
             ELSE ta.total_revenue * 1.0 / ta.unique_customers
        END                                        AS avg_spend_per_customer
    FROM tier_agg ta
    LEFT JOIN dim_membership_tier mt
      ON ta.membership_tier_sk = mt.membership_tier_sk
    ORDER BY total_revenue DESC;
    """
    rows = run_sql_dicts(sql)

    methodology = [
        "Treat each row in fact_transaction_header as a visit/transaction.",
        "Deduplicate on (pos_system, pos_transaction_id) to avoid multiple counts of the same visit.",
        "Group by membership_tier_sk to compute total transactions, unique_customers, and total_revenue.",
        "Join dim_membership_tier to attach human-readable tier names.",
        "Compute avg_visits_per_customer as transactions / unique_customers.",
        "Compute avg_spend_per_customer as total_revenue / unique_customers."
    ]

    assumptions = [
        "A customer can belong to only one membership tier per transaction in the unified model.",
        "Membership information is present and correct for POS systems that support it; others default to 'None'.",
        "Transactions with missing customer_sk are excluded from per-customer averages.",
        "Revenue is taken from total_amount on the header; line-level discounts are already applied upstream.",
        "Time-window effects (e.g., before vs after a membership program change) are not modeled in this basic view."
    ]

    return {
        "question": "Business Question 3: Membership Impact",
        "results": rows,
        "methodology": methodology,
        "assumptions": assumptions,
    }

@app.get("/analysis/schema_health")
def analysis_schema_health():
    """
    High-level view of schema health across all sources.

    Uses stg_unmapped_columns (populated by unify_pos.py) to show:
    - How many unmapped columns per (pos_system, source_table)
    - How many times we've seen them
    - When they were first and last detected

    If there are no rows, it means that (so far) all analyzed files
    matched the expected schemas with no extra columns.
    """
    sql = """
    SELECT
        pos_system,
        source_table,
        COUNT(DISTINCT column_name) AS unmapped_columns,
        COUNT(*)                    AS occurrences,
        MIN(detected_at)            AS first_seen,
        MAX(detected_at)            AS last_seen
    FROM stg_unmapped_columns
    GROUP BY pos_system, source_table
    ORDER BY pos_system, source_table;
    """

    try:
        per_source = run_sql_dicts(sql)
    except sqlite3.OperationalError as e:
        raise HTTPException(
            status_code=500,
            detail=(
                "stg_unmapped_columns table not found. "
                "Re-run unify_pos.py after updating the ETL with schema diagnostics."
            ),
        ) from e

    total_sources = len(per_source)
    total_unmapped_columns = sum(row["unmapped_columns"] for row in per_source) if per_source else 0

    status = "ok" if total_unmapped_columns == 0 else "needs_review"

    methodology = [
        "During ETL, unify_pos.py calls analyze_schema() for each source CSV.",
        "Any extra columns that are not mapped into the canonical dim/fact tables "
        "are recorded into stg_unmapped_columns along with a sample value and inferred type.",
        "This endpoint groups those diagnostics by (pos_system, source_table) to show "
        "where schemas are drifting away from the expected structure.",
    ]

    assumptions = [
        "If a source file has no unmapped columns, it will not appear in this report.",
        "The presence of unmapped columns does not automatically mean data is unusable; "
        "it signals that new fields exist which are not yet modeled in the star schema.",
        "This report does not yet auto-map new columns; it is meant as input to a human "
        "or a separate mapping/learning process.",
    ]

    return {
        "question": "Schema Health Across Sources",
        "status": status,
        "summary": {
            "total_sources_with_unmapped_columns": total_sources,
            "total_distinct_unmapped_columns": total_unmapped_columns,
        },
        "per_source": per_source,
        "methodology": methodology,
        "assumptions": assumptions,
    }


# -----------------------------------------------------
# Simple HTML frontend
# -----------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def home():
    html = """
    <html>
      <head><title>Unified POS Analytics</title></head>
      <body>
        <h1>Unified POS Analytics</h1>
        <h2>Business Question</h2>
        <select id="question-select">
          <option value="/revenue_by_location">Revenue by location</option>
          <option value="/revenue_by_location_detailed">Revenue by location (detailed)</option>
          <option value="/service_popularity">Service popularity (overall)</option>
          <option value="/service_popularity?by_location=true">Service popularity (by location)</option>
          <option value="/top_services_by_location">Top services by location</option>
          <option value="/customer_engagement_by_membership">Customer engagement by membership tier</option>
          <option value="/customer_frequency_by_membership">Customer frequency by membership tier</option>
          <option value="/membership_service_mix">Membership service mix</option>
          <option value="/summary_all">All summaries</option>
        </select>
        <button onclick="runQuery()">Run</button>

        <h2>Natural Language Query</h2>
        <input id="nl-input" size="80"
               placeholder="e.g., Which location has the highest revenue?" />
        <label>
          <input id="nl-all" type="checkbox" />
          Return all summaries
        </label>
        <button onclick="runNL()">Ask</button>

        <pre id="output"></pre>

        <script>
          async function runQuery() {
            const endpoint = document.getElementById('question-select').value;
            const resp = await fetch(endpoint);
            const data = await resp.json();
            document.getElementById('output').textContent =
              JSON.stringify(data, null, 2);
          }

          async function runNL() {
            const q = encodeURIComponent(
              document.getElementById('nl-input').value
            );
            const all = document.getElementById('nl-all').checked
              ? "&all=true"
              : "";
            const resp = await fetch(`/nl_query?q=${q}${all}`);
            const data = await resp.json();
            document.getElementById('output').textContent =
              JSON.stringify(data, null, 2);
          }
        </script>
      </body>
    </html>
    """
    return html

