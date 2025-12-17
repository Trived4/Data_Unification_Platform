# unify_pos.py
"""
End-to-end ETL to unify POS A/B/C data into a canonical star schema.

Usage:
    python unify_pos.py --data-dir ./data --db-path ./unified.db

Expected folder structure (flexible filenames):
    ./data/
      posa/
        db1/
          <account*.csv>
          <customers*.csv>
          <services*.csv>
          <transactions*.csv>
          [payments*.csv]
        db2/
          ...
      posb/
        db1/
          <locations*.csv>
          <customers*.csv>
          <service_catalog*.csv>
          <sales*.csv>
          <memberships*.csv>
          [payment_records*.csv]
      posc/
        db1/
          <location*.csv>
          <customer*.csv>
          <service*.csv>
          <transactionheader*.csv>
          <transactionlineitem*.csv>
          [payment*.csv]
        db2/
        db3/

File names can be anything, as long as they contain these keywords.
"""

import argparse
import os
import sqlite3
from collections import defaultdict
from typing import List, Optional, Dict, Any
from tqdm import tqdm
import math
import re
from collections import Counter


import pandas as pd

def log(msg: str):
    """Small helper to print progress messages immediately."""
    print(f"[INFO] {msg}", flush=True)


# ---------------------------------------------------
# Constants//Global Params
# ---------------------------------------------------

POSA = "POSA"
POSB = "POSB"
POSC = "POSC"
POSD = "POSD"


# ---------------------------------------------------
# Utility helpers
# ---------------------------------------------------

CANONICAL_SYNONYMS = {
    "location_id": ["location_id", "location_identifier", "loc_id", "loc_code", "store_id", "store_code"],
    "location_name": ["location_name", "loc_name", "store_name"],
    "city": ["city", "town"],
    "state": ["state", "province", "region"],
    "zip_code": ["zip_code", "postal_code", "postcode", "zip"],
    "customer_id": ["customer_id", "cust_id", "customer_token", "cust_token", "customer_uuid"],
    "first_name": ["first_name", "fname", "given_name"],
    "last_name": ["last_name", "lname", "surname", "family_name"],
    "email": ["email", "email_address"],
    "phone_number": ["phone_number", "phone", "mobile", "cell"],
    "service_id": ["service_id", "svc_id", "service_key", "service_uuid"],
    "service_name": ["service_name", "svc_name", "product_name"],
    "sale_id": ["sale_id", "transaction_id", "txn_id", "trans_id"],
    "sale_date": ["sale_date", "sold_at", "transaction_date", "txn_ts"],
    "amount": ["amount", "gross_amount", "total", "total_amount"],
    "sales_tax": ["sales_tax", "tax", "tax_component"],
    "discount_amount": ["discount_amount", "discount", "discount_value"],
    "created_at": ["created_at", "recorded_at", "created_on", "inserted_at"],
}


def find_file(data_dir: str, candidates: List[str]) -> str:
    """
    Legacy helper (not used by the new folder-based loader, but kept for reference).
    Given a list of possible file names (in priority order),
    return the first one that exists. Raise if none are found.
    """
    for name in candidates:
        path = os.path.join(data_dir, name)
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"None of the candidate files exist: {candidates}")


def safe_parse_datetime(series: pd.Series, invalid_sentinels=None) -> pd.Series:
    """
    Parse a pandas Series as datetimes, treating known sentinel values
    (like '1900-01-01') as invalid (NaT).
    """
    if invalid_sentinels is None:
        invalid_sentinels = {"1900-01-01", "1900-01-01 00:00:00"}
    raw = series.astype(str)
    out = pd.to_datetime(raw, errors="coerce")
    mask = raw.isin(invalid_sentinels)
    out[mask] = pd.NaT
    return out


def normalize_postal_code(series: pd.Series) -> pd.Series:
    """
    Convert zip/postal codes to clean strings (strip decimals and whitespace).
    """
    return (
        series.astype("string")
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )


def is_email_valid(email: str) -> Optional[bool]:
    """
    Very simple email validator:
    - If NULL -> None
    - If starts with 'INVALID_' -> False
    - If contains '@' -> True
    - Else False
    """
    if pd.isna(email):
        return None
    s = str(email)
    if s.startswith("INVALID_"):
        return False
    return "@" in s


def parse_bool(series: pd.Series) -> pd.Series:
    """
    Normalize different boolean representations into pandas Int64 (0/1/null).
    """
    def _one(x):
        if pd.isna(x):
            return None
        if isinstance(x, bool):
            return int(x)
        s = str(x).strip().lower()
        if s in ("1", "true", "t", "yes", "y"):
            return 1
        if s in ("0", "false", "f", "no", "n"):
            return 0
        return None

    return series.map(_one).astype("Int64")

def normalize_col_name(name: str) -> str:
    """
    Normalize a column name to a comparable form:
    - lowercase
    - remove non-alphanumeric characters

    Used for rough matching / diagnostics only.
    """
    import re
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def infer_series_type(series: pd.Series) -> str:
    """
    Very lightweight type inference for a pandas Series.
    Used only for diagnostics in stg_unmapped_columns.
    """
    if series.empty:
        return "unknown"

    # Pick the first non-null value
    non_null = series.dropna()
    if non_null.empty:
        return "unknown"

    v = non_null.iloc[0]

    # Try some basic checks
    if isinstance(v, (int, float)):
        return "numeric"
    if isinstance(v, bool):
        return "boolean"

    s = str(v)
    # Simple date-like detection
    date_like = any(ch in s for ch in ("-", "/", ":"))
    if date_like:
        return "maybe_datetime"

    if "@" in s:
        return "maybe_email"

    return "string"


def guess_series_type(series: pd.Series) -> str:
    """
    Heuristic type guess: 'numeric', 'datetime', or 'string'.
    Uses a small sample of non-null values.
    """
    sample = series.dropna().astype(str).head(50)
    if sample.empty:
        return "unknown"

    # numeric-ish?
    numeric_like = 0
    for v in sample:
        try:
            float(v)
            numeric_like += 1
        except ValueError:
            pass
    if numeric_like >= max(3, len(sample) * 0.7):
        return "numeric"

    # datetime-ish?
    dt_like = 0
    for v in sample:
        try:
            pd.to_datetime(v, errors="raise")
            dt_like += 1
        except Exception:
            pass
    if dt_like >= max(3, len(sample) * 0.7):
        return "datetime"

    return "string"


def record_unmapped_columns(
    conn: sqlite3.Connection,
    pos_system: str,
    source_table: str,
    df: pd.DataFrame,
    extra_cols: list[str],
):
    """
    Insert unmapped / unexpected source columns into stg_unmapped_columns
    for inspection. We store one sample value and an inferred type.
    """
    if not extra_cols:
        return

    cur = conn.cursor()

    rows_to_insert = []
    for col in extra_cols:
        if col not in df.columns:
            continue
        series = df[col]
        non_null = series.dropna()
        sample_value = None
        if not non_null.empty:
            sample_value = str(non_null.iloc[0])
        inferred_type = infer_series_type(series)
        rows_to_insert.append(
            (pos_system, source_table, col, sample_value, inferred_type)
        )

    if rows_to_insert:
        cur.executemany(
            """
            INSERT INTO stg_unmapped_columns
            (pos_system, source_table, column_name, sample_value, inferred_type)
            VALUES (?,?,?,?,?)
            """,
            rows_to_insert,
        )
        conn.commit()


def map_columns(df: pd.DataFrame, wanted_keys: List[str]) -> Dict[str, Optional[str]]:
    """
    For each canonical key in wanted_keys, try to find the best-matching df column.
    Returns dict: {canonical_key: actual_column_name or None}
    """
    cols = [c.lower() for c in df.columns]
    col_map: Dict[str, Optional[str]] = {}

    for key in wanted_keys:
        synonyms = CANONICAL_SYNONYMS.get(key, [key])
        found = None

        # 1) exact match
        for s in synonyms:
            if s in cols:
                found = df.columns[cols.index(s)]
                break

        # 2) substring fallback (very simple heuristic)
        if not found:
            for c in df.columns:
                lc = c.lower()
                if any(s in lc for s in synonyms):
                    found = c
                    break

        col_map[key] = found

    return col_map


def analyze_schema(
    conn: sqlite3.Connection,
    pos_system: str,
    source_table: str,
    df: pd.DataFrame,
    expected_cols: list[str],
    difference_threshold: float = 0.5,
):
    """
    Compare the actual columns from a CSV (df.columns) with the set of
    columns we actually use in the ETL for that source_table.

    - Logs:
        * extra columns not used by ETL
        * missing expected columns
        * a big warning if the schema differs by more than `difference_threshold`
          (default: 50% different).
    - Records extra columns into stg_unmapped_columns for later inspection.

    This is a *diagnostic* tool – it doesn't try to magically map columns
    into the model. It just tells you what doesn't fit and stores samples.
    """
    actual = set(df.columns)
    expected = set(expected_cols)

    matched = actual & expected
    extra = sorted(actual - expected)
    missing = sorted(expected - actual)

    union_size = len(actual | expected) or 1
    similarity = len(matched) / union_size
    difference = 1.0 - similarity

    if extra:
        log(
            f"[SCHEMA] {pos_system}::{source_table} has extra columns "
            f"not used by ETL: {extra}"
        )
        record_unmapped_columns(conn, pos_system, source_table, df, extra)

    if missing:
        log(
            f"[SCHEMA] {pos_system}::{source_table} is missing expected columns: "
            f"{missing}"
        )

    if difference > difference_threshold:
        log(
            f"[SCHEMA][ALERT] {pos_system}::{source_table} schema differs by "
            f"{difference:.0%} from the expected structure. "
            "Review mappings before trusting this data."
        )


def normalize_col_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def column_name_similarity(a: str, b: str) -> float:
    """
    Very cheap similarity: Jaccard over character sets + common prefix.
    0.0 – 1.0; higher is more similar.
    """
    na, nb = normalize_col_name(a), normalize_col_name(b)
    if not na or not nb:
        return 0.0
    set_a, set_b = set(na), set(nb)
    jacc = len(set_a & set_b) / max(1, len(set_a | set_b))
    # prefix bonus
    prefix_match = 1.0 if na.startswith(nb) or nb.startswith(na) else 0.0
    return 0.7 * jacc + 0.3 * prefix_match


def series_value_overlap(a: pd.Series, b: pd.Series) -> float:
    """
    Rough overlap score between two series (0–1).
    Only uses small sample for speed.
    """
    sample_a = set(a.dropna().astype(str).head(200))
    sample_b = set(b.dropna().astype(str).head(200))
    if not sample_a or not sample_b:
        return 0.0
    return len(sample_a & sample_b) / max(1, len(sample_a | sample_b))


def analyze_schema_against_canonical(
    df: pd.DataFrame,
    table_label: str,
    canonical_cols: List[str],
) -> dict:
    """
    Compare df.columns with canonical_cols.
    Returns:
        {
          'mapped': {df_col -> canonical_col},
          'unknown': [df_col, ...],
          'missing': [canonical_col, ...],
          'drift_ratio': float,
          'notes': [msg, ...],
        }
    """
    df_cols = list(df.columns)
    notes: List[str] = []

    # 1) direct matches after normalization
    norm_to_canonical = {}
    for c in canonical_cols:
        norm_to_canonical[normalize_col_name(c)] = c

    mapped: Dict[str, str] = {}
    unknown: List[str] = []

    for col in df_cols:
        norm = normalize_col_name(col)
        if norm in norm_to_canonical:
            mapped[col] = norm_to_canonical[norm]
        else:
            unknown.append(col)

    # 2) try fuzzy name mapping for unknowns
    still_unknown: List[str] = []
    for col in unknown:
        best_c = None
        best_score = 0.0
        for can in canonical_cols:
            score = column_name_similarity(col, can)
            if score > best_score:
                best_score, best_c = score, can
        if best_c and best_score >= 0.75:
            mapped[col] = best_c
            notes.append(
                f"[{table_label}] Fuzzy-mapped '{col}' -> '{best_c}' (name similarity {best_score:.2f})"
            )
        else:
            still_unknown.append(col)

    unknown = still_unknown

    # 3) compute missing canonical columns
    used_canonical = set(mapped.values())
    missing = [c for c in canonical_cols if c not in used_canonical]

    # 4) schema drift ratio
    total = len(set(df_cols) | set(canonical_cols))
    diff = len(set(df_cols) ^ set(canonical_cols))
    drift_ratio = diff / max(1, total)

    if drift_ratio > 0.5:
        notes.append(
            f"[{table_label}] WARNING: schema drift ratio {drift_ratio:.2f} (> 0.50)."
        )

    if unknown:
        notes.append(
            f"[{table_label}] New/unmapped columns detected: {', '.join(unknown)}"
        )
    if missing:
        notes.append(
            f"[{table_label}] Canonical columns missing from this file: {', '.join(missing)}"
        )

    return {
        "mapped": mapped,
        "unknown": unknown,
        "missing": missing,
        "drift_ratio": drift_ratio,
        "notes": notes,
    }

def record_posd_mapping(
    mappings: list[dict],
    *,
    db_name: str,
    source_table: str,
    source_column: str,
    target_table: str,
    target_column: str,
    sample_value: Any = None,
    confidence: float = 1.0,
):
    """
    Append one POSD column→target mapping into an in-memory list.
    We'll bulk-insert this list into stg_posd_mapping at the end
    of the POSD load.
    """
    mappings.append(
        {
            "pos_system": POSD,   # assuming you have POSD = "POSD"
            "db_name": db_name,
            "source_table": source_table,
            "source_column": source_column,
            "target_table": target_table,
            "target_column": target_column,
            "confidence": confidence,
            "sample_value": None if sample_value is None else str(sample_value),
        }
    )


# def ensure_raw_table_with_columns(
#     conn: sqlite3.Connection,
#     table_name: str,
#     df: pd.DataFrame,
# ):
#     """
#     Create or extend a 'raw' table so that *all* columns in df exist.
#     All columns are stored as TEXT for simplicity.
#     """
#     cur = conn.cursor()

#     # create if not exists with first column only
#     cur.execute(
#         f"""
#         CREATE TABLE IF NOT EXISTS {table_name} (
#             __raw_id INTEGER PRIMARY KEY AUTOINCREMENT
#         )
#         """
#     )

#     # existing columns
#     cur.execute(f"PRAGMA table_info({table_name})")
#     existing = {row[1] for row in cur.fetchall()}

#     for col in df.columns:
#         if col not in existing:
#             # add as TEXT
#             log(f"[POSD] Adding new raw column '{col}' to {table_name}")
#             cur.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{col}\" TEXT")

#     conn.commit()

#     # insert rows
#     # we include only columns that exist in table (safety)
#     cur.execute(f"PRAGMA table_info({table_name})")
#     existing = [row[1] for row in cur.fetchall() if row[1] != "__raw_id"]
#     insert_cols = [c for c in df.columns if c in existing]

#     if not insert_cols:
#         return

#     placeholders = ",".join(["?"] * len(insert_cols))
#     col_list = ",".join(f"\"{c}\"" for c in insert_cols)
#     rows = []
#     for _, r in df[insert_cols].iterrows():
#         rows.append(tuple(None if pd.isna(v) else str(v) for v in r))

#     cur.executemany(
#         f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
#         rows,
#     )
#     conn.commit()


# ---------------------------------------------------
# Source discovery (folders + filenames)
# ---------------------------------------------------

def detect_pos_system(path: str) -> str:
    """
    Infer POS system from a directory path.

    Works with folder names like:
      - posa, posa1, pos_a
      - posb, pos_b
      - posc, pos_c
      - posd, pos_d  (NEW)
    """
    p = path.lower().replace("_", "")

    if "posa" in p:
        return POSA
    if "posb" in p:
        return POSB
    if "posc" in p:
        return POSC
    if "posd" in p:      
        return POSD

    return ""


def classify_table_from_filename(system: str, filename: str) -> str:
    """
    Infer logical table type from filename based on simple keywords.
    You can extend this mapping as needed.

    Returns values like:
      POSA: 'account', 'customers', 'services', 'transactions', 'payments'
      POSB: 'locations', 'customers', 'service_catalog', 'sales', 'memberships', 'payment_records'
      POSC: 'location', 'customer', 'service', 'transactionheader', 'transactionlineitem', 'payment'
    """
    name = filename.lower()

    if system == POSA:
        if "account" in name:
            return "account"
        if "customer" in name:
            return "customers"
        if "service" in name:
            return "services"
        if "transaction" in name or "txn" in name:
            return "transactions"
        if "payment" in name:
            return "payments"

    if system in (POSB, POSD):
        if "location" in name:
            return "locations"
        if "customer" in name:
            return "customers"
        if "service_catalog" in name or ("service" in name and "catalog" in name):
            return "service_catalog"
        if "sale" in name:
            return "sales"
        if "membership" in name:
            return "memberships"
        if "payment" in name and "record" in name:
            return "payment_records"

    if system == POSC:
        if "location" in name:
            return "location"
        if "customer" in name:
            return "customer"
        if "service" in name:
            return "service"
        if "transactionheader" in name or "header" in name:
            return "transactionheader"
        if "transactionlineitem" in name or "lineitem" in name or "line_item" in name:
            return "transactionlineitem"
        if "payment" in name:
            return "payment"

    return ""  # unknown / ignore


def discover_sources(data_dir: str) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Discover POS source files under data_dir.

    Returns a nested dictionary:
    {
      "POSA": {
        "db1": { "account": "path/to/...", "customers": "...", ... },
        "db2": { ... }
      },
      "POSB": { "db1": { ... } },
      "POSC": { "db1": {...}, "db2": {...}, "db3": {...} }
    }

    - pos_system is inferred from the path (posa, posb, posc).
    - db_name is the last directory under the POS root (e.g., 'db1').
    - table types are inferred by keywords in filenames.
    """
    sources: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(lambda: defaultdict(dict))

    for root, dirs, files in os.walk(data_dir):
        system = detect_pos_system(root)
        if not system:
            continue

        # guess db_name as last part of the path under the POS folder
        # e.g., data/posa/db1 -> 'db1'
        path_parts = root.replace("\\", "/").split("/")
        db_name = path_parts[-1]

        for f in files:
            if not f.lower().endswith(".csv"):
                continue
            table_type = classify_table_from_filename(system, f)
            if not table_type:
                continue
            full_path = os.path.join(root, f)
            sources[system][db_name][table_type] = full_path

    return sources


# ---------------------------------------------------
# SQLite schema creation (DDL)
# ---------------------------------------------------

DDL_SQL = """
CREATE TABLE IF NOT EXISTS dim_location (
    location_sk     INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_system      TEXT NOT NULL,
    pos_location_id TEXT NOT NULL,
    location_code   TEXT,
    location_name   TEXT,
    address_line1   TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    phone_number    TEXT,
    created_at      TEXT,
    updated_at      TEXT,
    UNIQUE(pos_system, pos_location_id)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk     INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_system      TEXT NOT NULL,
    pos_customer_id TEXT NOT NULL,
    customer_guid   TEXT,
    first_name      TEXT,
    last_name       TEXT,
    email           TEXT,
    is_email_valid  INTEGER,
    phone_number    TEXT,
    address_line1   TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    is_active       INTEGER,
    created_at      TEXT,
    updated_at      TEXT,
    UNIQUE(pos_system, pos_customer_id)
);

CREATE TABLE IF NOT EXISTS dim_service (
    service_sk      INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_system      TEXT NOT NULL,
    pos_service_id  TEXT NOT NULL,
    service_code    TEXT,
    service_name    TEXT,
    service_category TEXT,
    base_price      REAL,
    is_active       INTEGER,
    created_at      TEXT,
    updated_at      TEXT,
    UNIQUE(pos_system, pos_service_id)
);

CREATE TABLE IF NOT EXISTS dim_membership_tier (
    membership_tier_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_system          TEXT,
    tier_name           TEXT NOT NULL,
    tier_description    TEXT
);

CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_sk     INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_system          TEXT,
    pos_payment_type_id TEXT,
    payment_type_name   TEXT
);

CREATE TABLE IF NOT EXISTS fact_transaction_header (
    transaction_header_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_system            TEXT NOT NULL,
    pos_transaction_id    TEXT NOT NULL,
    location_sk           INTEGER NOT NULL,
    customer_sk           INTEGER,
    membership_tier_sk    INTEGER,
    payment_type_sk       INTEGER,
    transaction_ts        TEXT,
    business_date         TEXT,
    subtotal_amount       REAL,
    tax_amount            REAL,
    discount_amount       REAL,
    total_amount          REAL,
    is_date_imputed       INTEGER DEFAULT 0,
    source_row_count      INTEGER DEFAULT 1,
    UNIQUE(pos_system, pos_transaction_id)
);

CREATE TABLE IF NOT EXISTS fact_transaction_line_item (
    transaction_line_sk   INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_header_sk INTEGER NOT NULL,
    service_sk            INTEGER NOT NULL,
    quantity              REAL NOT NULL,
    unit_price            REAL,
    line_amount           REAL,
    created_at            TEXT
);

-- Staging table to capture columns from source files
-- that are not currently mapped into the canonical model.
CREATE TABLE IF NOT EXISTS stg_unmapped_columns (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_system    TEXT NOT NULL,
    source_table  TEXT NOT NULL,
    column_name   TEXT NOT NULL,
    sample_value  TEXT,
    inferred_type TEXT,
    detected_at   TEXT DEFAULT (datetime('now'))
);

-- =========================================
-- POSD COLUMN → TARGET MAPPING METADATA
-- =========================================
CREATE TABLE IF NOT EXISTS stg_posd_mapping (
    mapping_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_system     TEXT NOT NULL,          -- e.g. 'POSD'
    db_name        TEXT,                   -- e.g. 'db1', 'db2'
    source_table   TEXT NOT NULL,          -- e.g. 'posd_db1_locations'
    source_column  TEXT NOT NULL,          -- e.g. 'LocationID'
    target_table   TEXT NOT NULL,          -- e.g. 'posd_dim_location'
    target_column  TEXT NOT NULL,          -- e.g. 'location_id'
    confidence     REAL NOT NULL DEFAULT 1.0,
    sample_value   TEXT,
    detected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

MAPPING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stg_posd_column_mapping (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_system     TEXT NOT NULL,          -- e.g. 'POSD'
    db_name        TEXT,                   -- e.g. 'db1', 'db2'
    source_table   TEXT NOT NULL,          -- e.g. 'POSD_db2_transactions'
    source_column  TEXT NOT NULL,          -- e.g. 'TxnDate'
    target_table   TEXT NOT NULL,          -- e.g. 'fact_transaction_header'
    target_column  TEXT NOT NULL,          -- e.g. 'business_date'
    confidence     REAL,                   -- 0.0–1.0
    detected_at    TEXT DEFAULT (datetime('now'))
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_dim_location_code
    ON dim_location (location_code);

CREATE INDEX IF NOT EXISTS idx_dim_customer_email
    ON dim_customer (email);

CREATE INDEX IF NOT EXISTS idx_dim_service_name
    ON dim_service (service_name);

CREATE INDEX IF NOT EXISTS idx_fact_header_location
    ON fact_transaction_header (location_sk);

CREATE INDEX IF NOT EXISTS idx_fact_header_customer
    ON fact_transaction_header (customer_sk);

CREATE INDEX IF NOT EXISTS idx_fact_header_date
    ON fact_transaction_header (business_date);

CREATE INDEX IF NOT EXISTS idx_fact_line_service
    ON fact_transaction_line_item (service_sk);
"""


def init_db(conn: sqlite3.Connection):
    """Create tables and seed 'None' membership tier."""
    conn.executescript(DDL_SQL)
    # Seed a canonical "None" membership tier with SK=1
    conn.executescript(INDEX_SQL)
    conn.execute(
        """
        INSERT OR IGNORE INTO dim_membership_tier
        (membership_tier_sk, pos_system, tier_name, tier_description)
        VALUES (1, NULL, 'None', 'No membership information available')
        """
    )
    conn.commit()


def init_mapping_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(MAPPING_TABLE_SQL)
    conn.commit()


# # ---------------------------------------------------
# # Generic "raw" table helper for unknown schemas
# # ---------------------------------------------------

# def ensure_raw_table_with_columns(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame):
#     """
#     Ensure a generic raw table exists with at least the columns in df.
#     Adds new columns as TEXT if they don't already exist.
#     """
#     cur = conn.cursor()

#     # create table if not exists with an autoincrement id
#     cur.execute(
#         f"""
#         CREATE TABLE IF NOT EXISTS {table_name} (
#             id INTEGER PRIMARY KEY AUTOINCREMENT
#         )
#         """
#     )

#     # existing columns from pragma
#     cur.execute(f"PRAGMA table_info({table_name})")
#     existing_cols = {row[1] for row in cur.fetchall()}  # row[1] is column name

#     for col in df.columns:
#         if col in existing_cols:
#             continue
#         # add new column as TEXT by default
#         cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT')
#         print(f"[WARN] Added new column '{col}' to raw table '{table_name}'", flush=True)

#     conn.commit()


# ---------------------------------------------------
# Raw “safety net” table helper
# ---------------------------------------------------

def ensure_raw_table_with_columns(
    conn: sqlite3.Connection,
    raw_table_name: str,
    df: pd.DataFrame,
    source_pos: str,
    source_db: str,
    source_file: str,
):
    """
    Generic RAW table loader for any POSD CSV.

    - Ensures a RAW table exists with base metadata columns.
    - Adds new columns as TEXT when it sees unseen df columns.
    - Inserts the df rows as TEXT values.
    """
    if df.empty:
        return

    cur = conn.cursor()

    # 1) Base table
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {raw_table_name} (
            raw_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            source_pos  TEXT,
            source_db   TEXT,
            source_file TEXT
            -- dynamic columns added later
        )
        """
    )

    # 2) What columns exist now?
    cur.execute(f"PRAGMA table_info({raw_table_name})")
    existing_cols = {row[1] for row in cur.fetchall()}

    # 3) Add any new df columns as TEXT
    new_cols = [c for c in df.columns if c not in existing_cols]
    for col in sorted(new_cols):
        try:
            # Quote the column just in case there are spaces or weird characters
            cur.execute(f'ALTER TABLE {raw_table_name} ADD COLUMN "{col}" TEXT')
        except sqlite3.OperationalError as e:
            # If the error is that the column already exists (case issues etc),
            # just log and move on. SQLite treats column names case-insensitively.
            if "duplicate column name" in str(e):
                log(f"[RAW] Column {col} already exists on {raw_table_name}, skipping add.")
            else:
                raise

    # 4) Insert data
    insert_cols = ["source_pos", "source_db", "source_file"] + [
        c for c in existing_cols if c not in ("raw_id", "source_pos", "source_db", "source_file")
    ]
    placeholders = ",".join(["?"] * len(insert_cols))

    def to_str(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return str(v)

    records = df.to_dict(orient="records")
    values = []
    for r in records:
        row_vals = [source_pos, source_db, source_file]
        for col in insert_cols[3:]:
            row_vals.append(to_str(r.get(col)))
        values.append(row_vals)

    cur.executemany(
        f"INSERT INTO {raw_table_name} ({','.join(insert_cols)}) VALUES ({placeholders})",
        values,
    )
    conn.commit()


#--------------------------------------------------
# Record Utility Helper
#--------------------------------------------------


def record_posd_mapping(
    conn: sqlite3.Connection,
    db_name: str,
    source_table: str,
    mappings: dict[str, tuple[str, str, float]],  # src_col -> (target_table, target_col, confidence)
    pos_system: str = "POSD",
) -> None:
    """
    Persist mapping decisions so the API can expose them later.
    Example mapping:
        {
          "TxnDate": ("fact_transaction_header", "business_date", 0.9),
          "StoreCode": ("dim_location", "location_code", 1.0),
        }
    """
    if not mappings:
        return

    rows = [
        (pos_system, db_name, source_table, src_col, tgt_tbl, tgt_col, conf)
        for src_col, (tgt_tbl, tgt_col, conf) in mappings.items()
    ]

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO stg_posd_column_mapping
        (pos_system, db_name, source_table, source_column, target_table, target_column, confidence)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


# ---------------------------------------------------
# Generic dimension upsert
# ---------------------------------------------------

def upsert_dim(conn: sqlite3.Connection, table: str, key_cols: List[str], df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        sk_col = table.replace("dim_", "") + "_sk"
        df[sk_col] = pd.Series(dtype="int64")
        return df

    cols = list(df.columns)
    col_list = ",".join(cols)
    placeholders = ",".join(["?"] * len(cols))

    cur = conn.cursor()

    def to_db_value(v):
        try:
            if pd.isna(v):
                return None
        except TypeError:
            pass
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        if hasattr(v, "item"):
            try:
                return v.item()
            except Exception:
                pass
        return v

    # ---------- BULK INSERT ----------
    # Prepare all rows as a list of tuples and insert in one go
    value_rows = [
        tuple(to_db_value(row[c]) for c in cols)
        for _, row in df.iterrows()
    ]

    cur.executemany(
        f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({placeholders})",
        value_rows,
    )
    conn.commit()

    # ---------- FETCH KEYS BACK ----------
    key_predicate = " AND ".join([f"{c} = ?" for c in key_cols])
    sk_col = table.replace("dim_", "") + "_sk"
    sk_values = []

    for _, row in df.iterrows():
        key_values = [to_db_value(row[c]) for c in key_cols]
        cur.execute(
            f"SELECT {sk_col} FROM {table} WHERE {key_predicate}",
            tuple(key_values),
        )
        res = cur.fetchone()
        if res is None:
            raise RuntimeError(f"Could not find {table} row for keys {key_cols}={key_values}")
        sk_values.append(res[0])

    df[sk_col] = sk_values
    return df


# ---------------------------------------------------
# POS A loader: from discovered files
# ---------------------------------------------------

def load_pos_a_from_files(
    files: Dict[str, str],
    conn: sqlite3.Connection,
    chunksize: int = 50_000,  # tuning this for bigger datasets
):
    """
    Load a single POS A database given its discovered files dict, using chunked
    processing for large tables (customers, transactions).

    Expected keys in `files`: 'account', 'customers', 'services', 'transactions'
    """
    required = {"account", "customers", "services", "transactions"}
    if not required.issubset(files.keys()):
        log(f"[WARN] POSA source missing required files: {required - set(files.keys())}. Skipping.")
        return

    system = POSA
    cur = conn.cursor()

    # -----------------------------
    # Read small/static tables fully
    # -----------------------------
    log(f"[POSA] Reading account file: {files['account']}")
    account = pd.read_csv(files["account"])

    log(f"[POSA] Reading services file: {files['services']}")
    services = pd.read_csv(files["services"])

    #------------------------------------------
    # Schema diagnostics for POSA static tables
    #------------------------------------------
    analyze_schema(
        conn=conn,
        pos_system=system,
        source_table="posa.account",
        df=account,
        expected_cols=[
            "lLocationID",
            "sLocationDesc",
            "sLocationName",
            "sAddress",
            "sCity",
            "sState",
            "sZipCode",
            "sPhone",
            "dtCreated",
            "dtModified",
        ],
    )

    analyze_schema(
        conn=conn,
        pos_system=system,
        source_table="posa.services",
        df=services,
        expected_cols=[
            "lServiceID",
            "sServiceCode",
            "sServiceName",
            "dblBasePrice",
            "bIsActive",
            "dtCreated",
            "dtModified",
        ],
    )

    # -----------------------------
    # dim_location (from account)
    # -----------------------------
    log("[POSA] Building dim_location")
    dim_loc = pd.DataFrame({
        "pos_system": system,
        "pos_location_id": account["lLocationID"].astype(str),
        "location_code": account["sLocationDesc"],
        "location_name": account["sLocationName"],
        "address_line1": account["sAddress"],
        "city": account["sCity"],
        "state": account["sState"],
        "postal_code": normalize_postal_code(account["sZipCode"]),
        "phone_number": account["sPhone"],
        "created_at": account["dtCreated"],
        "updated_at": account["dtModified"],
    }).drop_duplicates(subset=["pos_system", "pos_location_id"])

    dim_loc = upsert_dim(conn, "dim_location", ["pos_system", "pos_location_id"], dim_loc)
    loc_map = dim_loc.set_index("pos_location_id")["location_sk"].to_dict()
    log(f"[POSA] dim_location rows this batch: {len(dim_loc)}")

    # -----------------------------
    # dim_service (from services)
    # -----------------------------
    log("[POSA] Building dim_service")
    dim_serv = pd.DataFrame({
        "pos_system": system,
        "pos_service_id": services["lServiceID"].astype(str),
        "service_code": services["sServiceCode"],
        "service_name": services["sServiceName"],
        # rough category from name
        "service_category": services["sServiceName"].astype(str).str.extract(r"^(Wash|Dry)", expand=False),
        "base_price": services["dblBasePrice"],
        "is_active": parse_bool(services["bIsActive"]),
        "created_at": services["dtCreated"],
        "updated_at": services["dtModified"],
    }).drop_duplicates(subset=["pos_system", "pos_service_id"])

    dim_serv = upsert_dim(conn, "dim_service", ["pos_system", "pos_service_id"], dim_serv)
    serv_map = dim_serv.set_index("pos_service_id")["service_sk"].to_dict()
    log(f"[POSA] dim_service rows this batch: {len(dim_serv)}")

    # -----------------------------
    # dim_customer in chunks
    # -----------------------------
    log(f"[POSA] Building dim_customer from {files['customers']} in chunks of {chunksize}")
    cust_map: Dict[str, int] = {}  # pos_customer_id -> customer_sk

    for i, cust_chunk in enumerate(pd.read_csv(files["customers"], chunksize=chunksize)):
        log(f"[POSA] Processing customers chunk {i}, rows={len(cust_chunk)}")

        if i == 0:
            # Only analyze schema on the first chunk (same columns for all chunks)
            analyze_schema(
                conn=conn,
                pos_system=system,
                source_table="posa.customers",
                df=cust_chunk,
                expected_cols=[
                    "lCustomerID",
                    "CustomerGUID",
                    "sFirstName",
                    "sLastName",
                    "sEmail",
                    "sPhone",
                    "sAddress",
                    "sCity",
                    "sState",
                    "sZipCode",
                    "bIsActive",
                    "dtCreated",
                    "dtModified",
                ],
            )

        cust_chunk = cust_chunk.copy()
        cust_chunk["email_valid"] = cust_chunk["sEmail"].apply(is_email_valid)

        dim_cust_chunk = pd.DataFrame({
            "pos_system": system,
            "pos_customer_id": cust_chunk["lCustomerID"].astype(str),
            "customer_guid": cust_chunk["CustomerGUID"],
            "first_name": cust_chunk["sFirstName"],
            "last_name": cust_chunk["sLastName"],
            "email": cust_chunk["sEmail"],
            "is_email_valid": cust_chunk["email_valid"].astype("Int64"),
            "phone_number": cust_chunk["sPhone"],
            "address_line1": cust_chunk["sAddress"],
            "city": cust_chunk["sCity"],
            "state": cust_chunk["sState"],
            "postal_code": normalize_postal_code(cust_chunk["sZipCode"]),
            "is_active": parse_bool(cust_chunk["bIsActive"]),
            "created_at": cust_chunk["dtCreated"],
            "updated_at": cust_chunk["dtModified"],
        }).drop_duplicates(subset=["pos_system", "pos_customer_id"])

        dim_cust_chunk = upsert_dim(conn, "dim_customer", ["pos_system", "pos_customer_id"], dim_cust_chunk)

        # extend the global customer_sk lookup
        chunk_map = dim_cust_chunk.set_index("pos_customer_id")["customer_sk"].to_dict()
        cust_map.update(chunk_map)

        log(f"[POSA] dim_customer rows added in chunk {i}: {len(dim_cust_chunk)}")

    log(f"[POSA] Finished dim_customer. Total unique customers SKs in map: {len(cust_map)}")

    # -----------------------------
    # Transactions in chunks
    # -----------------------------
    log(f"[POSA] Loading transactions from {files['transactions']} in chunks of {chunksize}")

    for i, tx_chunk in enumerate(pd.read_csv(files["transactions"], chunksize=chunksize)):
        log(f"[POSA] Processing transactions chunk {i}, rows={len(tx_chunk)}")

        if i == 0:
            analyze_schema(
                conn=conn,
                pos_system=system,
                source_table="posa.transactions",
                df=tx_chunk,
                expected_cols=[
                    "lTransactionID",
                    "lLocationID",
                    "lCustomerID",
                    "lServiceID",
                    "dtTransactionDate",
                    "dtCreated",
                    "dblAmount",
                ],
            )

        tx = tx_chunk.copy()

        # parse & normalize dates
        tx["TransactionDateParsed"] = safe_parse_datetime(tx["dtTransactionDate"])
        created_parsed = safe_parse_datetime(tx["dtCreated"])
        tx["FinalTransactionDate"] = tx["TransactionDateParsed"].fillna(created_parsed)
        tx["business_date"] = tx["FinalTransactionDate"].dt.date.astype(str)
        tx["pos_transaction_id"] = tx["lTransactionID"].astype(str)

        # -------------------------
        # Build and insert headers
        # -------------------------
        header_rows = []
        for _, row in tx.iterrows():
            loc_sk = loc_map[str(row["lLocationID"])]

            cust_sk = None
            if not pd.isna(row["lCustomerID"]):
                cust_sk = cust_map.get(str(int(row["lCustomerID"])), None)

            dt_val = row["FinalTransactionDate"]
            is_imputed = 0 if pd.notna(row["TransactionDateParsed"]) else 1

            header_rows.append({
                "pos_system": system,
                "pos_transaction_id": str(row["lTransactionID"]),
                "location_sk": loc_sk,
                "customer_sk": cust_sk,
                "membership_tier_sk": 1,  # 'None'
                "payment_type_sk": None,
                "transaction_ts": dt_val.isoformat() if pd.notna(dt_val) else None,
                "business_date": str(row["business_date"]),
                "subtotal_amount": row["dblAmount"],
                "tax_amount": None,
                "discount_amount": None,
                "total_amount": row["dblAmount"],
                "is_date_imputed": is_imputed,
                "source_row_count": 1,
            })

        hdr_df = pd.DataFrame(header_rows)

        if hdr_df.empty:
            log(f"[POSA] No headers to insert for chunk {i}, skipping.")
            continue

        # bulk insert headers
        cur.executemany(
            """
            INSERT OR IGNORE INTO fact_transaction_header
            (pos_system,pos_transaction_id,location_sk,customer_sk,membership_tier_sk,
             payment_type_sk,transaction_ts,business_date,subtotal_amount,tax_amount,
             discount_amount,total_amount,is_date_imputed,source_row_count)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    r["pos_system"], r["pos_transaction_id"], r["location_sk"],
                    r["customer_sk"], r["membership_tier_sk"], r["payment_type_sk"],
                    r["transaction_ts"], r["business_date"], r["subtotal_amount"],
                    r["tax_amount"], r["discount_amount"], r["total_amount"],
                    r["is_date_imputed"], r["source_row_count"],
                )
                for _, r in hdr_df.iterrows()
            ],
        )
        conn.commit()

        # -------------------------
        # Fetch SKs for this chunk's headers in bulk
        # -------------------------
        tx_ids = [r["pos_transaction_id"] for _, r in hdr_df.iterrows()]
        hdr_sk_map_chunk: Dict[str, int] = {}

        if tx_ids:
            placeholders = ",".join(["?"] * len(tx_ids))
            cur.execute(
                f"""
                SELECT pos_transaction_id, transaction_header_sk
                FROM fact_transaction_header
                WHERE pos_system = ?
                  AND pos_transaction_id IN ({placeholders})
                """,
                [system, *tx_ids],
            )
            for pos_tx_id, sk in cur.fetchall():
                hdr_sk_map_chunk[pos_tx_id] = sk

        # -------------------------
        # Build and insert line items
        # -------------------------
        line_rows = []
        for _, row in tx.iterrows():
            t_id = str(row["lTransactionID"])
            header_sk = hdr_sk_map_chunk.get(t_id)
            if header_sk is None:
                # header didn't get inserted / found; skip this line
                continue

            service_sk = serv_map[str(row["lServiceID"])]
            amt = row["dblAmount"]

            line_rows.append({
                "transaction_header_sk": header_sk,
                "service_sk": service_sk,
                "quantity": 1.0,
                "unit_price": amt,
                "line_amount": amt,
                "created_at": row["dtCreated"],
            })

        if not line_rows:
            log(f"[POSA] No line items to insert for chunk {i}, skipping.")
            continue

        line_df = pd.DataFrame(line_rows)

        cur.executemany(
            """
            INSERT INTO fact_transaction_line_item
            (transaction_header_sk,service_sk,quantity,unit_price,line_amount,created_at)
            VALUES (?,?,?,?,?,?)
            """,
            [
                (
                    r["transaction_header_sk"], r["service_sk"], r["quantity"],
                    r["unit_price"], r["line_amount"], r["created_at"],
                )
                for _, r in line_df.iterrows()
            ],
        )
        conn.commit()

        log(f"[POSA] Finished transactions chunk {i}: headers={len(hdr_df)}, lines={len(line_df)}")

    log("[POSA] Finished POSA db")


# ---------------------------------------------------
# POS B loader: from discovered files
# ---------------------------------------------------

def load_pos_b_from_files(
    files: Dict[str, str],
    conn: sqlite3.Connection,
    chunksize: int = 50_000,
    system: str = POSB,          #which POS code to stamp into the warehouse
    system_label: Optional[str] = None,  #how to show it in logs
):
    """
    Load a single POS B (or POSB-style) database given its discovered files dict,
    using chunked processing for large tables (customers, sales).

    Expected keys in `files`: 'locations', 'customers', 'service_catalog', 'sales', 'memberships'
    """
    if system_label is None:
        system_label = system

    required = {"locations", "customers", "service_catalog", "sales"}
    missing = required - set(files.keys())
    if missing:
        log(f"[{system_label}] source missing required files: {missing}. Skipping.")
        return

    # system will be POSB or POSD depending on caller
    cur = conn.cursor()


    # -----------------------------
    # Read small/static tables fully
    # -----------------------------
    log(f"[POSB] Reading locations file: {files['locations']}")
    locations = pd.read_csv(files["locations"])

    log(f"[POSB] Reading service_catalog file: {files['service_catalog']}")
    services = pd.read_csv(files["service_catalog"])

    analyze_schema(
        conn=conn,
        pos_system=system,
        source_table="posb.locations",
        df=locations,
        expected_cols=[
            "location_id",
            "location_code",
            "location_name",
            "address",
            "city",
            "state",
            "zip_code",
            "phone",
            "created_at",
            "updated_at",
        ],
    )

    analyze_schema(
        conn=conn,
        pos_system=system,
        source_table="posb.service_catalog",
        df=services,
        expected_cols=[
            "service_id",
            "service_code",
            "service_name",
            "base_price",
            "is_active",
            "created_at",
            "updated_at",
        ],
    )

    memberships = None
    if "memberships" in files:
        log(f"[POSB] Reading memberships file: {files['memberships']}")
        memberships = pd.read_csv(files["memberships"])
    else:
        log("[POSB] No memberships file found. All customers treated as 'None' tier.")

    # -----------------------------
    # dim_location
    # -----------------------------
    log("[POSB] Building dim_location")
    dim_loc = pd.DataFrame({
        "pos_system": system,
        "pos_location_id": locations["location_id"].astype(str),
        "location_code": locations["location_code"],
        "location_name": locations["location_name"],
        "address_line1": locations["address"],
        "city": locations["city"],
        "state": locations["state"],
        "postal_code": normalize_postal_code(locations["zip_code"]),
        "phone_number": locations["phone"],
        "created_at": locations["created_at"],
        "updated_at": locations["updated_at"],
    }).drop_duplicates(subset=["pos_system", "pos_location_id"])

    dim_loc = upsert_dim(conn, "dim_location", ["pos_system", "pos_location_id"], dim_loc)
    loc_map = dim_loc.set_index("pos_location_id")["location_sk"].to_dict()
    log(f"[POSB] dim_location rows this batch: {len(dim_loc)}")

    # -----------------------------
    # dim_service
    # -----------------------------
    log("[POSB] Building dim_service")
    dim_serv = pd.DataFrame({
        "pos_system": system,
        "pos_service_id": services["service_id"].astype(str),
        "service_code": services["service_code"],
        "service_name": services["service_name"],
        "service_category": services["service_name"].astype(str).str.extract(r"^(Wash|Dry)", expand=False),
        "base_price": services["base_price"],
        "is_active": parse_bool(services["is_active"]),
        "created_at": services["created_at"],
        "updated_at": services["updated_at"],
    }).drop_duplicates(subset=["pos_system", "pos_service_id"])

    dim_serv = upsert_dim(conn, "dim_service", ["pos_system", "pos_service_id"], dim_serv)
    serv_map = dim_serv.set_index("pos_service_id")["service_sk"].to_dict()
    log(f"[POSB] dim_service rows this batch: {len(dim_serv)}")

    # -----------------------------
    # dim_membership_tier
    # -----------------------------
    # Seed 'None' is already in init_db as SK=1. Now load POSB tiers.
    tier_map = {"None": 1}

    if memberships is not None and not memberships.empty:
        tiers = memberships[["tier_name"]].drop_duplicates()
        for _, row in tiers.iterrows():
            name = row["tier_name"]
            cur.execute(
                "INSERT OR IGNORE INTO dim_membership_tier (pos_system, tier_name, tier_description) VALUES (?, ?, ?)",
                (system, name, f"Tier from {system}")
            )
        conn.commit()

        # build tier_map
        cur.execute(
            "SELECT membership_tier_sk, tier_name FROM dim_membership_tier WHERE pos_system = ? OR pos_system IS NULL",
            (system,),
        )
        for sk, name in cur.fetchall():
            tier_map[name] = sk

        # preprocess membership date ranges
        memberships["start_date_parsed"] = pd.to_datetime(memberships["start_date"], errors="coerce")
        memberships["end_date_parsed"] = pd.to_datetime(memberships["end_date"], errors="coerce")
    else:
        memberships = None  # ensure None

    def find_tier_for_tx(cust_id, tx_dt):
        """Return membership_tier_sk for a given customer and transaction timestamp."""
        if memberships is None or pd.isna(cust_id) or pd.isna(tx_dt):
            return tier_map["None"]
        sub = memberships[memberships["customer_id"] == cust_id]
        if sub.empty:
            return tier_map["None"]
        mask = (
            (sub["start_date_parsed"] <= tx_dt)
            & (sub["end_date_parsed"].isna() | (sub["end_date_parsed"] >= tx_dt))
        )
        sub2 = sub[mask]
        if sub2.empty:
            return tier_map["None"]
        tier_name = sub2.iloc[0]["tier_name"]
        return tier_map.get(tier_name, tier_map["None"])

    # -----------------------------
    # dim_customer in chunks
    # -----------------------------
    log(f"[POSB] Building dim_customer from {files['customers']} in chunks of {chunksize}")
    cust_map: Dict[str, int] = {}  # pos_customer_id -> customer_sk

    for i, cust_chunk in enumerate(pd.read_csv(files["customers"], chunksize=chunksize)):
        log(f"[POSB] Processing customers chunk {i}, rows={len(cust_chunk)}")

        if i == 0:
            analyze_schema(
                conn=conn,
                pos_system=system,
                source_table="posb.customers",
                df=cust_chunk,
                expected_cols=[
                    "customer_id",
                    "customer_guid",
                    "first_name",
                    "last_name",
                    "email",
                    "phone_number",
                    "address",
                    "city",
                    "state",
                    "zip_code",
                    "is_active",
                    "created_at",
                    "updated_at",
                ],
            )

        cust_chunk = cust_chunk.copy()
        cust_chunk["email_valid"] = cust_chunk["email"].apply(is_email_valid)

        dim_cust_chunk = pd.DataFrame({
            "pos_system": system,
            "pos_customer_id": cust_chunk["customer_id"].astype(str),
            "customer_guid": cust_chunk["customer_guid"],
            "first_name": cust_chunk["first_name"],
            "last_name": cust_chunk["last_name"],
            "email": cust_chunk["email"],
            "is_email_valid": cust_chunk["email_valid"].astype("Int64"),
            "phone_number": cust_chunk["phone_number"],
            "address_line1": cust_chunk["address"],
            "city": cust_chunk["city"],
            "state": cust_chunk["state"],
            "postal_code": normalize_postal_code(cust_chunk["zip_code"]),
            "is_active": parse_bool(cust_chunk["is_active"]),
            "created_at": cust_chunk["created_at"],
            "updated_at": cust_chunk["updated_at"],
        }).drop_duplicates(subset=["pos_system", "pos_customer_id"])

        dim_cust_chunk = upsert_dim(conn, "dim_customer", ["pos_system", "pos_customer_id"], dim_cust_chunk)
        chunk_map = dim_cust_chunk.set_index("pos_customer_id")["customer_sk"].to_dict()
        cust_map.update(chunk_map)

        log(f"[POSB] dim_customer rows added in chunk {i}: {len(dim_cust_chunk)}")

    log(f"[POSB] Finished dim_customer. Total unique customer SKs: {len(cust_map)}")

    # -----------------------------
    # sales (transactions) in chunks
    # -----------------------------
    log(f"[POSB] Loading sales from {files['sales']} in chunks of {chunksize}")

    for i, sales_chunk in enumerate(pd.read_csv(files["sales"], chunksize=chunksize)):
        log(f"[POSB] Processing sales chunk {i}, rows={len(sales_chunk)}")

        if i == 0:
            analyze_schema(
                conn=conn,
                pos_system=system,
                source_table="posb.sales",
                df=sales_chunk,
                expected_cols=[
                    "sale_id",
                    "location_id",
                    "customer_id",
                    "service_id",
                    "sale_date",
                    "amount",
                    "sales_tax",
                    "discount_amount",
                    "created_at",
                ],
            )

        s = sales_chunk.copy()
        s["sale_dt_parsed"] = safe_parse_datetime(s["sale_date"])
        s["business_date"] = s["sale_dt_parsed"].dt.date.astype(str)
        s["pos_transaction_id"] = s["sale_id"].astype(str)

        # -------------------------
        # Build headers
        # -------------------------
        header_rows = []
        for _, row in s.iterrows():
            loc_sk = loc_map[str(row["location_id"])]
            cust_sk = None
            if not pd.isna(row["customer_id"]):
                cust_sk = cust_map.get(str(int(row["customer_id"])), None)

            tx_dt = row["sale_dt_parsed"]
            tier_sk = find_tier_for_tx(row["customer_id"], tx_dt) if pd.notna(tx_dt) else tier_map["None"]

            # handle tax & subtotal
            subtotal = row["amount"]
            tax = row.get("sales_tax", None)
            if not pd.isna(tax):
                subtotal = row["amount"] - tax

            header_rows.append({
                "pos_system": system,
                "pos_transaction_id": str(row["sale_id"]),
                "location_sk": loc_sk,
                "customer_sk": cust_sk,
                "membership_tier_sk": tier_sk,
                "payment_type_sk": None,  # could map from payment_method_id
                "transaction_ts": tx_dt.isoformat() if pd.notna(tx_dt) else None,
                "business_date": row["business_date"],
                "subtotal_amount": subtotal,
                "tax_amount": tax,
                "discount_amount": row.get("discount_amount", None),
                "total_amount": row["amount"],
                "is_date_imputed": 0 if pd.notna(tx_dt) else 1,
                "source_row_count": 1,
            })

        hdr_df = pd.DataFrame(header_rows)
        if hdr_df.empty:
            log(f"[POSB] No headers to insert for sales chunk {i}, skipping.")
            continue

        # bulk insert headers
        cur.executemany(
            """
            INSERT OR IGNORE INTO fact_transaction_header
            (pos_system,pos_transaction_id,location_sk,customer_sk,membership_tier_sk,
             payment_type_sk,transaction_ts,business_date,subtotal_amount,tax_amount,
             discount_amount,total_amount,is_date_imputed,source_row_count)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    r["pos_system"], r["pos_transaction_id"], r["location_sk"],
                    r["customer_sk"], r["membership_tier_sk"], r["payment_type_sk"],
                    r["transaction_ts"], r["business_date"], r["subtotal_amount"],
                    r["tax_amount"], r["discount_amount"], r["total_amount"],
                    r["is_date_imputed"], r["source_row_count"],
                )
                for _, r in hdr_df.iterrows()
            ],
        )
        conn.commit()

        # fetch SKs for this chunk's transactions
        tx_ids = [r["pos_transaction_id"] for _, r in hdr_df.iterrows()]
        hdr_sk_map_chunk: Dict[str, int] = {}

        if tx_ids:
            placeholders = ",".join(["?"] * len(tx_ids))
            cur.execute(
                f"""
                SELECT pos_transaction_id, transaction_header_sk
                FROM fact_transaction_header
                WHERE pos_system = ?
                  AND pos_transaction_id IN ({placeholders})
                """,
                [system, *tx_ids],
            )
            for pos_tx_id, sk in cur.fetchall():
                hdr_sk_map_chunk[pos_tx_id] = sk

        # -------------------------
        # Build line items (one per sale)
        # -------------------------
        line_rows = []
        for _, row in s.iterrows():
            t_id = str(row["sale_id"])
            header_sk = hdr_sk_map_chunk.get(t_id)
            if header_sk is None:
                continue

            service_sk = serv_map[str(row["service_id"])]
            amt = row["amount"]
            line_rows.append({
                "transaction_header_sk": header_sk,
                "service_sk": service_sk,
                "quantity": 1.0,
                "unit_price": amt,
                "line_amount": amt,
                "created_at": row["created_at"],
            })

        if not line_rows:
            log(f"[POSB] No line items to insert for sales chunk {i}, skipping.")
            continue

        line_df = pd.DataFrame(line_rows)
        cur.executemany(
            """
            INSERT INTO fact_transaction_line_item
            (transaction_header_sk,service_sk,quantity,unit_price,line_amount,created_at)
            VALUES (?,?,?,?,?,?)
            """,
            [
                (
                    r["transaction_header_sk"], r["service_sk"], r["quantity"],
                    r["unit_price"], r["line_amount"], r["created_at"],
                )
                for _, r in line_df.iterrows()
            ],
        )
        conn.commit()

        log(f"[POSB] Finished sales chunk {i}: headers={len(hdr_df)}, lines={len(line_df)}")

    log("[POSB] Finished POSB db")

    if system == POSB:
        if "location" in name:
            return "locations"
        if "customer" in name:
            return "customers"
        if "service_catalog" in name or ("service" in name and "catalog" in name):
            return "service_catalog"
        if "sale" in name:
            return "sales"
        if "membership" in name:
            return "memberships"
        if "payment" in name and "record" in name:
            return "payment_records"
        
    # if system == POSD:
    #     if "location" in name:
    #         return "locations"
    #     if "customer" in name:
    #         return "customers"
    #     if "service_catalog" in name or ("service" in name and "catalog" in name):
    #         return "service_catalog"
    #     if "sale" in name:
    #         return "sales"
    #     if "membership" in name:
    #         return "memberships"
    #     if "payment" in name and "record" in name:
    #         return "payment_records"


# ---------------------------------------------------
# POS C loader: from discovered files
# ---------------------------------------------------

def load_pos_c_from_files(
    files: Dict[str, str],
    conn: sqlite3.Connection,
    chunksize: int = 50_000,
):
    """
    Load a single POS C database given its discovered files dict.
    Uses chunked processing for customers and batched inserts for facts.

    Expected keys in `files`: 'location', 'customer', 'service', 'transactionheader', 'transactionlineitem'
    """
    required = {"location", "customer", "service", "transactionheader", "transactionlineitem"}
    missing = required - set(files.keys())
    if missing:
        log(f"[WARN] POSC source missing required files: {missing}. Skipping.")
        return

    system = POSC
    cur = conn.cursor()

    # -----------------------------
    # Read small/static tables fully
    # -----------------------------
    log(f"[POSC] Reading location file: {files['location']}")
    loc = pd.read_csv(files["location"])

    log(f"[POSC] Reading service file: {files['service']}")
    serv = pd.read_csv(files["service"])

    log(f"[POSC] Reading transactionheader file: {files['transactionheader']}")
    th = pd.read_csv(files["transactionheader"])

    log(f"[POSC] Reading transactionlineitem file: {files['transactionlineitem']}")
    tl = pd.read_csv(files["transactionlineitem"])

    analyze_schema(
        conn=conn,
        pos_system=system,
        source_table="posc.location",
        df=loc,
        expected_cols=[
            "LocationID",
            "LocationCode",
            "LocationName",
            "Address",
            "City",
            "State",
            "ZipCode",
            "Phone",
            "CreatedDate",
            "ModifiedDate",
        ],
    )

    analyze_schema(
        conn=conn,
        pos_system=system,
        source_table="posc.service",
        df=serv,
        expected_cols=[
            "ServiceID",
            "ServiceCode",
            "ServiceName",
            # ServiceCategory / category variants handled later,
            "BasePrice",
            "Active",
            "CreatedDate",
            "ModifiedDate",
        ],
    )

    # -----------------------------
    # dim_location
    # -----------------------------
    log("[POSC] Building dim_location")
    dim_loc = pd.DataFrame({
        "pos_system": system,
        "pos_location_id": loc["LocationID"].astype(str),
        "location_code": loc["LocationCode"],
        "location_name": loc["LocationName"],
        "address_line1": loc["Address"],
        "city": loc["City"],
        "state": loc["State"],
        "postal_code": normalize_postal_code(loc["ZipCode"]),
        "phone_number": loc["Phone"],
        "created_at": loc["CreatedDate"],
        "updated_at": loc["ModifiedDate"],
    }).drop_duplicates(subset=["pos_system", "pos_location_id"])

    dim_loc = upsert_dim(conn, "dim_location", ["pos_system", "pos_location_id"], dim_loc)
    loc_map = dim_loc.set_index("pos_location_id")["location_sk"].to_dict()
    log(f"[POSC] dim_location rows this batch: {len(dim_loc)}")

    # -----------------------------
    # dim_service
    # -----------------------------
    log("[POSC] Building dim_service")
    # ServiceCategory may be missing; handle flexibly
    if "ServiceCategory" in serv.columns:
        service_category_series = serv["ServiceCategory"]
    elif "service_category" in serv.columns:
        service_category_series = serv["service_category"]
    elif "Category" in serv.columns:
        service_category_series = serv["Category"]
    elif "category" in serv.columns:
        service_category_series = serv["category"]
    else:
        service_category_series = serv["ServiceName"].astype(str).str.extract(r"^(Wash|Dry)", expand=False)

    dim_serv = pd.DataFrame({
        "pos_system": system,
        "pos_service_id": serv["ServiceID"].astype(str),
        "service_code": serv["ServiceCode"],
        "service_name": serv["ServiceName"],
        "service_category": service_category_series,
        "base_price": serv["BasePrice"],
        "is_active": parse_bool(serv["Active"]),
        "created_at": serv["CreatedDate"],
        "updated_at": serv["ModifiedDate"],
    }).drop_duplicates(subset=["pos_system", "pos_service_id"])

    dim_serv = upsert_dim(conn, "dim_service", ["pos_system", "pos_service_id"], dim_serv)
    serv_map = dim_serv.set_index("pos_service_id")["service_sk"].to_dict()
    log(f"[POSC] dim_service rows this batch: {len(dim_serv)}")

    # -----------------------------
    # dim_customer in chunks
    # -----------------------------
    log(f"[POSC] Building dim_customer from {files['customer']} in chunks of {chunksize}")
    cust_map: Dict[str, int] = {}  # pos_customer_id -> customer_sk

    for i, cust_chunk in enumerate(pd.read_csv(files["customer"], chunksize=chunksize)):
        log(f"[POSC] Processing customers chunk {i}, rows={len(cust_chunk)}")

        if i == 0:
            analyze_schema(
                conn=conn,
                pos_system=system,
                source_table="posc.customer",
                df=cust_chunk,
                expected_cols=[
                    "CustomerID",
                    "CustomerGUID",
                    "FirstName",
                    "LastName",
                    "EmailAddress",
                    "PhoneNumber",
                    "Address",
                    "City",
                    "State",
                    "ZipCode",
                    "Active",
                    "CreatedDate",
                    "ModifiedDate",
                ],
            )

        cust_chunk = cust_chunk.copy()
        cust_chunk["email_valid"] = cust_chunk["EmailAddress"].apply(is_email_valid)

        dim_cust_chunk = pd.DataFrame({
            "pos_system": system,
            "pos_customer_id": cust_chunk["CustomerID"].astype(str),
            "customer_guid": cust_chunk["CustomerGUID"],
            "first_name": cust_chunk["FirstName"],
            "last_name": cust_chunk["LastName"],
            "email": cust_chunk["EmailAddress"],
            "is_email_valid": cust_chunk["email_valid"].astype("Int64"),
            "phone_number": cust_chunk["PhoneNumber"],
            "address_line1": cust_chunk["Address"],
            "city": cust_chunk["City"],
            "state": cust_chunk["State"],
            "postal_code": normalize_postal_code(cust_chunk["ZipCode"]),
            "is_active": parse_bool(cust_chunk["Active"]),
            "created_at": cust_chunk["CreatedDate"],
            "updated_at": cust_chunk["ModifiedDate"],
        }).drop_duplicates(subset=["pos_system", "pos_customer_id"])

        dim_cust_chunk = upsert_dim(conn, "dim_customer", ["pos_system", "pos_customer_id"], dim_cust_chunk)
        chunk_map = dim_cust_chunk.set_index("pos_customer_id")["customer_sk"].to_dict()
        cust_map.update(chunk_map)

        log(f"[POSC] dim_customer rows added in chunk {i}: {len(dim_cust_chunk)}")

    log(f"[POSC] Finished dim_customer. Total unique customer SKs: {len(cust_map)}")

    # -----------------------------
    # fact_transaction_header
    # -----------------------------
    log("[POSC] Building fact_transaction_header from transactionheader")
    th = th.copy()
    th["TransactionDateParsed"] = safe_parse_datetime(th["TransactionDate"])
    created_parsed = safe_parse_datetime(th["CreatedDate"])
    th["FinalTransactionDate"] = th["TransactionDateParsed"].fillna(created_parsed)
    th["business_date"] = th["FinalTransactionDate"].dt.date.astype(str)
    th["pos_transaction_id"] = th["TransactionHeaderID"].astype(str)

    header_rows = []
    for _, row in th.iterrows():
        loc_sk = loc_map[str(row["LocationID"])]

        cust_id = row["CustomerID"]
        cust_sk = None
        if not pd.isna(cust_id):
            cust_sk = cust_map.get(str(int(cust_id)), None)

        tx_dt = row["FinalTransactionDate"]
        is_imputed = 0 if pd.notna(row["TransactionDateParsed"]) else 1

        header_rows.append({
            "pos_system": system,
            "pos_transaction_id": str(row["TransactionHeaderID"]),
            "location_sk": loc_sk,
            "customer_sk": cust_sk,
            "membership_tier_sk": 1,  # 'None' – POSC has no membership
            "payment_type_sk": None,
            "transaction_ts": tx_dt.isoformat() if pd.notna(tx_dt) else None,
            "business_date": row["business_date"],
            "subtotal_amount": None,  # can be derived from line items
            "tax_amount": None,
            "discount_amount": None,
            "total_amount": row["TotalAmount"],
            "is_date_imputed": is_imputed,
            "source_row_count": 1,
        })

    hdr_df = pd.DataFrame(header_rows)
    if hdr_df.empty:
        log("[POSC] No transaction headers to insert. Skipping POSC fact load.")
        return

    cur.executemany(
        """
        INSERT OR IGNORE INTO fact_transaction_header
        (pos_system,pos_transaction_id,location_sk,customer_sk,membership_tier_sk,
         payment_type_sk,transaction_ts,business_date,subtotal_amount,tax_amount,
         discount_amount,total_amount,is_date_imputed,source_row_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                r["pos_system"], r["pos_transaction_id"], r["location_sk"],
                r["customer_sk"], r["membership_tier_sk"], r["payment_type_sk"],
                r["transaction_ts"], r["business_date"], r["subtotal_amount"],
                r["tax_amount"], r["discount_amount"], r["total_amount"],
                r["is_date_imputed"], r["source_row_count"],
            )
            for _, r in hdr_df.iterrows()
        ],
    )
    conn.commit()

    # Map all header SKs for this POSC DB
    hdr_sk_map: Dict[str, int] = {}
    cur.execute(
        """
        SELECT pos_transaction_id, transaction_header_sk
        FROM fact_transaction_header
        WHERE pos_system = ?
        """,
        (system,),
    )
    for pos_tx_id, sk in cur.fetchall():
        hdr_sk_map[pos_tx_id] = sk

    # -----------------------------
    # fact_transaction_line_item
    # -----------------------------
    log("[POSC] Building fact_transaction_line_item from transactionlineitem")
    tl = tl.copy()

    line_rows = []
    for _, row in tl.iterrows():
        header_id = str(row["TransactionHeaderID"])
        header_sk = hdr_sk_map.get(header_id)
        if header_sk is None:
            continue

        service_sk = serv_map[str(row["ServiceID"])]
        line_rows.append({
            "transaction_header_sk": header_sk,
            "service_sk": service_sk,
            "quantity": row["Quantity"],
            "unit_price": row["UnitPrice"],
            "line_amount": row["LineTotal"],
            "created_at": row["CreatedDate"],
        })

    if not line_rows:
        log("[POSC] No line items to insert.")
        return

    line_df = pd.DataFrame(line_rows)
    cur.executemany(
        """
        INSERT INTO fact_transaction_line_item
        (transaction_header_sk,service_sk,quantity,unit_price,line_amount,created_at)
        VALUES (?,?,?,?,?,?)
        """,
        [
            (
                r["transaction_header_sk"], r["service_sk"], r["quantity"],
                r["unit_price"], r["line_amount"], r["created_at"],
            )
            for _, r in line_df.iterrows()
        ],
    )
    conn.commit()

    log("[POSC] Finished POSC db")

#--------------------------------------------
#Generic Schema Recorder Helper
#--------------------------------------------
def record_generic_schema(
    conn: sqlite3.Connection,
    pos_system: str,
    source_table: str,
    df: pd.DataFrame,
) -> None:
    """
    Very lightweight schema recorder used for 'unknown' POS systems.

    We don't have a canonical column list here, so EVERY column is recorded
    into stg_unmapped_columns. That way /schema_diff and
    /analysis/schema_health can surface what POSD sent us.

    This does NOT attempt any intelligent mapping – it's just inventory.
    """
    if df.empty:
        return

    cur = conn.cursor()

    # Make sure the diagnostics staging table exists.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_unmapped_columns (
            pos_system    TEXT NOT NULL,
            source_table  TEXT NOT NULL,
            column_name   TEXT NOT NULL,
            inferred_type TEXT,
            sample_value  TEXT,
            detected_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    for col in df.columns:
        series = df[col]

        # Very cheap "sample" and type
        sample_val = None
        non_na = series[series.notna()]
        if not non_na.empty:
            sample_val = str(non_na.iloc[0])

        inferred_type = str(series.dtype)

        cur.execute(
            """
            INSERT INTO stg_unmapped_columns
            (pos_system, source_table, column_name, inferred_type, sample_value)
            VALUES (?, ?, ?, ?, ?)
            """,
            (pos_system, source_table, col, inferred_type, sample_val),
        )

    conn.commit()


# ---------------------------------------------------
# POS D loader: reuse POSB-style schema
# ---------------------------------------------------

def load_pos_d_from_files(
    files: Dict[str, str],
    conn: sqlite3.Connection,
    chunksize: int = 50_000,
):
    """
    Generic POSD loader.

    1) Stage ALL files into raw_posd_* tables using ensure_raw_table_with_columns.
    2) Try best-effort mapping to canonical dimensions/facts using map_columns.
    3) Record column mappings into stg_posd_mapping for /posd_mapping.
    4) Record schema diffs via analyze_schema for /schema_diff.
    5) If we can't confidently infer core pieces, we stop after raw staging.
    """

    system = POSD
    cur = conn.cursor()

    # -----------------------------
    # Derive a simple db_name (DB1/DB2/DB3)
    # -----------------------------
    any_path = next(iter(files.values()))
    base = os.path.basename(any_path)  # e.g. "posd_db2_transactions.csv"
    db_name = "UNKNOWN"
    m = re.search(r"db(\d+)", base, flags=re.I)
    if m:
        db_name = f"DB{m.group(1)}"

    # Small helper to insert mappings directly into stg_posd_mapping
    def insert_mapping(
        source_table: str,
        source_column: str,
        target_table: str,
        target_column: str,
        sample_value: Optional[str],
        confidence: float,
    ):
        cur.execute(
            """
            INSERT INTO stg_posd_mapping
            (pos_system, db_name, source_table, source_column,
             target_table, target_column, confidence, sample_value, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                system,
                db_name,
                source_table,
                source_column,
                target_table,
                target_column,
                confidence,
                None if sample_value is None else str(sample_value),
            ),
        )

    # -----------------------------
    # 1) RAW STAGING FOR EVERY FILE
    # -----------------------------
    for logical_name, path in files.items():
        df = pd.read_csv(path)
        raw_table = f"raw_posd_{logical_name.lower()}"
        log(f"[POSD] Staging {logical_name} -> {raw_table} (rows={len(df)})")
        ensure_raw_table_with_columns(
            conn=conn,
            raw_table_name=raw_table,
            df=df,
            source_pos=system,
            source_db=db_name,
            source_file=os.path.basename(path),
        )

    # -----------------------------
    # 2) Identify “roles” for the POSD files
    # -----------------------------
    locations_df = None
    customers_df = None
    services_df = None
    sales_df = None
    locations_name = None
    customers_name = None
    services_name = None
    sales_name = None

    for logical_name, path in files.items():
        df = pd.read_csv(path)
        lname = logical_name.lower()

        if any(t in lname for t in ["location", "store", "site", "account"]):
            locations_df = df
            locations_name = logical_name
        elif "customer" in lname or "client" in lname:
            customers_df = df
            customers_name = logical_name
        elif "service" in lname or "catalog" in lname or "product" in lname:
            services_df = df
            services_name = logical_name
        elif any(t in lname for t in ["sale", "transaction", "txn", "header"]):
            sales_df = df
            sales_name = logical_name

    # If we don't even have locations + services + sales, just stop after RAW.
    if locations_df is None or services_df is None or sales_df is None:
        log(
            "[POSD] Missing one of locations/services/sales. "
            "Keeping RAW-only for POSD; no canonical load attempted."
        )
        return

    # -----------------------------
    # 3) Schema diagnostics for POSD
    # -----------------------------
    # These calls populate stg_unmapped_columns so /schema_diff works.
    analyze_schema(
        conn=conn,
        pos_system=system,
        source_table=f"{db_name}.{locations_name}",
        df=locations_df,
        expected_cols=[
            "location_id",
            "location_code",
            "location_name",
            "address_line1",
            "city",
            "state",
            "zip_code",
            "phone_number",
            "created_at",
            "updated_at",
        ],
    )

    analyze_schema(
        conn=conn,
        pos_system=system,
        source_table=f"{db_name}.{services_name}",
        df=services_df,
        expected_cols=[
            "service_id",
            "service_code",
            "service_name",
            "base_price",
            "is_active",
            "created_at",
            "updated_at",
        ],
    )

    if customers_df is not None:
        analyze_schema(
            conn=conn,
            pos_system=system,
            source_table=f"{db_name}.{customers_name}",
            df=customers_df,
            expected_cols=[
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "phone_number",
                "address_line1",
                "city",
                "state",
                "zip_code",
                "is_active",
                "created_at",
                "updated_at",
            ],
        )

    analyze_schema(
        conn=conn,
        pos_system=system,
        source_table=f"{db_name}.{sales_name}",
        df=sales_df,
        expected_cols=[
            "sale_id",
            "location_id",
            "customer_id",
            "service_id",
            "sale_date",
            "amount",
            "sales_tax",
            "discount_amount",
            "created_at",
        ],
    )

    # -----------------------------
    # 4) dim_location
    # -----------------------------
    loc_keys = [
        "location_id",
        "location_code",
        "location_name",
        "address_line1",
        "city",
        "state",
        "zip_code",
        "phone_number",
        "created_at",
        "updated_at",
    ]
    loc_map = map_columns(locations_df, loc_keys)  # canon_key -> src_column or None

    matched_loc_cols = sum(1 for v in loc_map.values() if v is not None)
    coverage = matched_loc_cols / len(loc_keys)
    if coverage < 0.4:
        log(
            f"[POSD] Location coverage too low ({coverage:.0%}). "
            "Skipping canonical POSD load; RAW-only."
        )
        return

    # record mappings for /posd_mapping
    for canon_key, src_col in loc_map.items():
        if src_col is None:
            continue
        non_null = locations_df[src_col].dropna()
        sample_val = non_null.iloc[0] if not non_null.empty else None
        insert_mapping(
            source_table=locations_name,
            source_column=src_col,
            target_table="dim_location",
            target_column=canon_key,
            sample_value=sample_val,
            confidence=1.0,
        )

    dim_loc = pd.DataFrame(
        {
            "pos_system": system,
            "pos_location_id": locations_df[loc_map["location_id"]].astype(str)
            if loc_map["location_id"]
            else locations_df.index.astype(str),
            "location_code": (
                locations_df[loc_map["location_code"]]
                if loc_map["location_code"]
                else None
            ),
            "location_name": (
                locations_df[loc_map["location_name"]]
                if loc_map["location_name"]
                else None
            ),
            "address_line1": (
                locations_df[loc_map["address_line1"]]
                if loc_map["address_line1"]
                else None
            ),
            "city": locations_df[loc_map["city"]] if loc_map["city"] else None,
            "state": locations_df[loc_map["state"]] if loc_map["state"] else None,
            "postal_code": normalize_postal_code(
                locations_df[loc_map["zip_code"]]
            )
            if loc_map["zip_code"]
            else None,
            "phone_number": (
                locations_df[loc_map["phone_number"]]
                if loc_map["phone_number"]
                else None
            ),
            "created_at": (
                locations_df[loc_map["created_at"]]
                if loc_map["created_at"]
                else None
            ),
            "updated_at": (
                locations_df[loc_map["updated_at"]]
                if loc_map["updated_at"]
                else None
            ),
        }
    ).drop_duplicates(subset=["pos_system", "pos_location_id"])

    dim_loc = upsert_dim(
        conn, "dim_location", ["pos_system", "pos_location_id"], dim_loc
    )
    loc_sk_map = dim_loc.set_index("pos_location_id")["location_sk"].to_dict()
    log(f"[POSD] dim_location rows: {len(dim_loc)}")

    # -----------------------------
    # 5) dim_service
    # -----------------------------
    svc_keys = [
        "service_id",
        "service_name",
        "base_price",
        "is_active",
        "created_at",
        "updated_at",
    ]
    svc_map = map_columns(services_df, svc_keys)

    for canon_key, src_col in svc_map.items():
        if src_col is None:
            continue
        non_null = services_df[src_col].dropna()
        sample_val = non_null.iloc[0] if not non_null.empty else None
        insert_mapping(
            source_table=services_name,
            source_column=src_col,
            target_table="dim_service",
            target_column=canon_key,
            sample_value=sample_val,
            confidence=1.0,
        )

    dim_serv = pd.DataFrame(
        {
            "pos_system": system,
            "pos_service_id": services_df[svc_map["service_id"]].astype(str)
            if svc_map["service_id"]
            else services_df.index.astype(str),
            "service_code": (
                services_df[svc_map["service_id"]]
                if svc_map["service_id"]
                else None
            ),
            "service_name": (
                services_df[svc_map["service_name"]]
                if svc_map["service_name"]
                else None
            ),
            "service_category": (
                services_df[svc_map["service_name"]]
                .astype(str)
                .str.extract(r"^(Wash|Dry)", expand=False)
                if svc_map["service_name"]
                else None
            ),
            "base_price": (
                services_df[svc_map["base_price"]]
                if svc_map["base_price"]
                else None
            ),
            "is_active": (
                parse_bool(services_df[svc_map["is_active"]])
                if svc_map["is_active"]
                else None
            ),
            "created_at": (
                services_df[svc_map["created_at"]]
                if svc_map["created_at"]
                else None
            ),
            "updated_at": (
                services_df[svc_map["updated_at"]]
                if svc_map["updated_at"]
                else None
            ),
        }
    ).drop_duplicates(subset=["pos_system", "pos_service_id"])

    dim_serv = upsert_dim(
        conn, "dim_service", ["pos_system", "pos_service_id"], dim_serv
    )
    serv_sk_map = dim_serv.set_index("pos_service_id")["service_sk"].to_dict()
    log(f"[POSD] dim_service rows: {len(dim_serv)}")

    # -----------------------------
    # 6) dim_customer (optional)
    # -----------------------------
    cust_sk_map: Dict[str, int] = {}
    if customers_df is not None:
        cust_keys = [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone_number",
            "address_line1",
            "city",
            "state",
            "zip_code",
            "is_active",
            "created_at",
            "updated_at",
        ]
        cust_map = map_columns(customers_df, cust_keys)

        for canon_key, src_col in cust_map.items():
            if src_col is None:
                continue
            non_null = customers_df[src_col].dropna()
            sample_val = non_null.iloc[0] if not non_null.empty else None
            insert_mapping(
                source_table=customers_name,
                source_column=src_col,
                target_table="dim_customer",
                target_column=canon_key,
                sample_value=sample_val,
                confidence=1.0,
            )

        customers_df = customers_df.copy()
        if cust_map["email"]:
            customers_df["email_valid"] = customers_df[
                cust_map["email"]
            ].apply(is_email_valid)
        else:
            customers_df["email_valid"] = None

        dim_cust = pd.DataFrame(
            {
                "pos_system": system,
                "pos_customer_id": customers_df[
                    cust_map["customer_id"]
                ].astype(str)
                if cust_map["customer_id"]
                else customers_df.index.astype(str),
                "customer_guid": None,
                "first_name": (
                    customers_df[cust_map["first_name"]]
                    if cust_map["first_name"]
                    else None
                ),
                "last_name": (
                    customers_df[cust_map["last_name"]]
                    if cust_map["last_name"]
                    else None
                ),
                "email": (
                    customers_df[cust_map["email"]]
                    if cust_map["email"]
                    else None
                ),
                "is_email_valid": customers_df["email_valid"].astype("Int64"),
                "phone_number": (
                    customers_df[cust_map["phone_number"]]
                    if cust_map["phone_number"]
                    else None
                ),
                "address_line1": (
                    customers_df[cust_map["address_line1"]]
                    if cust_map["address_line1"]
                    else None
                ),
                "city": (
                    customers_df[cust_map["city"]]
                    if cust_map["city"]
                    else None
                ),
                "state": (
                    customers_df[cust_map["state"]]
                    if cust_map["state"]
                    else None
                ),
                "postal_code": normalize_postal_code(
                    customers_df[cust_map["zip_code"]]
                )
                if cust_map["zip_code"]
                else None,
                "is_active": (
                    parse_bool(customers_df[cust_map["is_active"]])
                    if cust_map["is_active"]
                    else None
                ),
                "created_at": (
                    customers_df[cust_map["created_at"]]
                    if cust_map["created_at"]
                    else None
                ),
                "updated_at": (
                    customers_df[cust_map["updated_at"]]
                    if cust_map["updated_at"]
                    else None
                ),
            }
        ).drop_duplicates(subset=["pos_system", "pos_customer_id"])

        dim_cust = upsert_dim(
            conn, "dim_customer", ["pos_system", "pos_customer_id"], dim_cust
        )
        cust_sk_map = dim_cust.set_index("pos_customer_id")[
            "customer_sk"
        ].to_dict()
        log(f"[POSD] dim_customer rows: {len(dim_cust)}")

    # -----------------------------
    # 7) fact_transaction_header + line_item
    # -----------------------------
    sale_keys = [
        "sale_id",
        "location_id",
        "customer_id",
        "service_id",
        "sale_date",
        "amount",
        "sales_tax",
        "discount_amount",
        "created_at",
    ]
    sale_map = map_columns(sales_df, sale_keys)

    for canon_key, src_col in sale_map.items():
        if src_col is None:
            continue
        non_null = sales_df[src_col].dropna()
        sample_val = non_null.iloc[0] if not non_null.empty else None
        insert_mapping(
            source_table=sales_name,
            source_column=src_col,
            target_table="fact_transaction_header",
            target_column=canon_key,
            sample_value=sample_val,
            confidence=1.0,
        )

    s = sales_df.copy()
    sale_dt_col = sale_map["sale_date"]
    if sale_dt_col:
        s["sale_dt_parsed"] = safe_parse_datetime(s[sale_dt_col])
        s["business_date"] = s["sale_dt_parsed"].dt.date.astype(str)
    else:
        s["sale_dt_parsed"] = pd.NaT
        s["business_date"] = None

    s["pos_transaction_id"] = (
        s[sale_map["sale_id"]].astype(str)
        if sale_map["sale_id"]
        else s.index.astype(str)
    )

    header_rows = []
    for _, row in s.iterrows():
        raw_loc_id = (
            row[sale_map["location_id"]] if sale_map["location_id"] else None
        )
        loc_sk = (
            loc_sk_map.get(str(raw_loc_id), None)
            if raw_loc_id is not None
            else None
        )
        if loc_sk is None:
            continue  # can't place header without location

        cust_sk = None
        if customers_df is not None and sale_map["customer_id"]:
            raw_cust_id = row[sale_map["customer_id"]]
            if not pd.isna(raw_cust_id):
                cust_sk = cust_sk_map.get(str(raw_cust_id), None)

        tx_dt = row["sale_dt_parsed"]
        amount = (
            row[sale_map["amount"]] if sale_map["amount"] else None
        )
        tax = (
            row[sale_map["sales_tax"]] if sale_map["sales_tax"] else None
        )
        discount = (
            row[sale_map["discount_amount"]]
            if sale_map["discount_amount"]
            else None
        )

        subtotal = amount
        if subtotal is not None and tax is not None and not pd.isna(tax):
            subtotal = subtotal - tax

        header_rows.append(
            {
                "pos_system": system,
                "pos_transaction_id": str(row["pos_transaction_id"]),
                "location_sk": loc_sk,
                "customer_sk": cust_sk,
                "membership_tier_sk": 1,  # None tier
                "payment_type_sk": None,
                "transaction_ts": tx_dt.isoformat()
                if pd.notna(tx_dt)
                else None,
                "business_date": row["business_date"],
                "subtotal_amount": subtotal,
                "tax_amount": tax,
                "discount_amount": discount,
                "total_amount": amount,
                "is_date_imputed": 0 if pd.notna(tx_dt) else 1,
                "source_row_count": 1,
            }
        )

    hdr_df = pd.DataFrame(header_rows)
    if hdr_df.empty:
        log("[POSD] No valid headers inferred; stopping canonical POSD load.")
        return

    cur.executemany(
        """
        INSERT OR IGNORE INTO fact_transaction_header
        (pos_system,pos_transaction_id,location_sk,customer_sk,membership_tier_sk,
         payment_type_sk,transaction_ts,business_date,subtotal_amount,tax_amount,
         discount_amount,total_amount,is_date_imputed,source_row_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                r["pos_system"],
                r["pos_transaction_id"],
                r["location_sk"],
                r["customer_sk"],
                r["membership_tier_sk"],
                r["payment_type_sk"],
                r["transaction_ts"],
                r["business_date"],
                r["subtotal_amount"],
                r["tax_amount"],
                r["discount_amount"],
                r["total_amount"],
                r["is_date_imputed"],
                r["source_row_count"],
            )
            for _, r in hdr_df.iterrows()
        ],
    )
    conn.commit()

    # Build map pos_transaction_id -> SK
    tx_ids = [r["pos_transaction_id"] for _, r in hdr_df.iterrows()]
    hdr_sk_map: Dict[str, int] = {}
    if tx_ids:
        placeholders = ",".join(["?"] * len(tx_ids))
        cur.execute(
            f"""
            SELECT pos_transaction_id, transaction_header_sk
            FROM fact_transaction_header
            WHERE pos_system = ?
              AND pos_transaction_id IN ({placeholders})
            """,
            [system, *tx_ids],
        )
        for pos_tx_id, sk in cur.fetchall():
            hdr_sk_map[pos_tx_id] = sk

    # line items: one per sale
    line_rows = []
    for _, row in s.iterrows():
        t_id = str(row["pos_transaction_id"])
        header_sk = hdr_sk_map.get(t_id)
        if header_sk is None:
            continue

        svc_id_raw = (
            row[sale_map["service_id"]] if sale_map["service_id"] else None
        )
        if svc_id_raw is None:
            continue

        service_sk = serv_sk_map.get(str(svc_id_raw))
        if service_sk is None:
            continue

        amt = row[sale_map["amount"]] if sale_map["amount"] else None
        line_rows.append(
            {
                "transaction_header_sk": header_sk,
                "service_sk": service_sk,
                "quantity": 1.0,
                "unit_price": amt,
                "line_amount": amt,
                "created_at": row[sale_map["created_at"]]
                if sale_map["created_at"]
                else None,
            }
        )

    if line_rows:
        line_df = pd.DataFrame(line_rows)
        cur.executemany(
            """
            INSERT INTO fact_transaction_line_item
            (transaction_header_sk,service_sk,quantity,unit_price,line_amount,created_at)
            VALUES (?,?,?,?,?,?)
            """,
            [
                (
                    r["transaction_header_sk"],
                    r["service_sk"],
                    r["quantity"],
                    r["unit_price"],
                    r["line_amount"],
                    r["created_at"],
                )
                for _, r in line_df.iterrows()
            ],
        )
        conn.commit()
        log(
            f"[POSD] Canonical POSD load complete: "
            f"headers={len(hdr_df)}, lines={len(line_df)}"
        )
    else:
        log(
            "[POSD] No line items inferred; you still have raw_posd_* "
            "for further analysis."
        )


# ---------------------------------------------------
# Main entrypoint
# ---------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, help="Root directory containing POS folders (pos_a, pos_b, pos_c)")
    parser.add_argument("--db-path", default="unified.db", help="SQLite database path")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    init_db(conn)
    init_mapping_tables(conn)
    

    log(f"Discovering sources under: {args.data_dir}")
    sources = discover_sources(args.data_dir)

    log("Discovered sources:")
    for system, dbs in sources.items():
        for db_name, files in dbs.items():
            log(f"  {system} / {db_name}: {list(files.keys())}")

    # POSA: iterate all db folders we discovered
    for db_name, files in sources.get(POSA, {}).items():
        log(f"Loading POSA {db_name} from files: {list(files.keys())}")
        load_pos_a_from_files(files, conn)

    # POSB: usually only db1
    for db_name, files in sources.get(POSB, {}).items():
        log(f"Loading POSB {db_name} from files: {list(files.keys())}")
        load_pos_b_from_files(files, conn)

    # POSC: db1, db2, db3
    for db_name, files in sources.get(POSC, {}).items():
        log(f"Loading POSC {db_name} from files: {list(files.keys())}")
        load_pos_c_from_files(files, conn)

    # POSD: POSB-style schema//replicating
    for db_name, files in sources.get(POSD, {}).items():
        log(f"Loading POSD {db_name} from files: {list(files.keys())}")
        load_pos_d_from_files(files, conn)

    conn.close()
    log(f"ETL completed. Unified DB at: {args.db_path}")



if __name__ == "__main__":
    main()
