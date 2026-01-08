import argparse
import csv
import io
import os
import sqlite3
import sys
import time
from pathlib import Path


TABLES = [
    {
        "name": "stock_basic",
        "columns": [
            "code",
            "name",
            "sector",
            "industry",
            "float_mv",
            "total_mv",
            "pe_ttm",
            "last_updated",
        ],
    },
    {
        "name": "daily_market",
        "columns": [
            "code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover_rate",
        ],
    },
    {
        "name": "margin_data",
        "columns": [
            "code",
            "trade_date",
            "financing_buy",
            "financing_balance",
            "securities_sell",
            "securities_balance",
            "net_financing_buy",
        ],
    },
    {
        "name": "northbound_data",
        "columns": [
            "code",
            "trade_date",
            "net_inflow",
        ],
    },
    {
        "name": "main_fund_flow",
        "columns": [
            "code",
            "trade_date",
            "main_net_inflow",
        ],
    },
    {
        "name": "trade_account",
        "columns": [
            "user_id",
            "cash",
            "total_assets",
            "updated_at",
        ],
    },
    {
        "name": "trade_positions",
        "columns": [
            "user_id",
            "code",
            "name",
            "quantity",
            "avg_cost",
            "updated_at",
        ],
    },
    {
        "name": "trade_orders",
        "columns": [
            "id",
            "user_id",
            "trade_date",
            "code",
            "name",
            "action",
            "price",
            "quantity",
            "amount",
            "commission",
            "balance_after",
            "created_at",
        ],
        "post_import": "fix_trade_orders_sequence",
    },
]


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _drop_optional_indexes(pg_cursor):
    for idx in [
        "idx_daily_market_trade_date",
        "idx_margin_data_trade_date",
        "idx_northbound_data_trade_date",
        "idx_main_fund_flow_trade_date",
    ]:
        pg_cursor.execute(f'DROP INDEX IF EXISTS {_quote_ident(idx)}')


def _create_optional_indexes(pg_cursor):
    pg_cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_daily_market_trade_date ON daily_market(trade_date)"
    )
    pg_cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_margin_data_trade_date ON margin_data(trade_date)"
    )
    pg_cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_northbound_data_trade_date ON northbound_data(trade_date)"
    )
    pg_cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_main_fund_flow_trade_date ON main_fund_flow(trade_date)"
    )


def _table_has_rows(pg_cursor, table: str) -> bool:
    pg_cursor.execute(f"SELECT 1 FROM {_quote_ident(table)} LIMIT 1")
    return pg_cursor.fetchone() is not None


def _fix_trade_orders_sequence(pg_cursor):
    pg_cursor.execute("SELECT pg_get_serial_sequence('trade_orders','id')")
    seq = pg_cursor.fetchone()
    seq_name = seq[0] if seq else None
    if not seq_name:
        return
    pg_cursor.execute("SELECT COALESCE(MAX(id), 0) FROM trade_orders")
    max_id = int(pg_cursor.fetchone()[0] or 0)
    # If max_id == 0, setting is_called=True will make nextval() return 1.
    pg_cursor.execute("SELECT setval(%s, %s, true)", (seq_name, max_id))


def _copy_table(
    *,
    sqlite_conn,
    pg_conn,
    table: str,
    columns: list[str],
    chunk_rows: int,
):
    sqlite_cur = sqlite_conn.cursor()
    cols_sql = ", ".join(_quote_ident(c) for c in columns)
    sqlite_cur.execute(f"SELECT {cols_sql} FROM {table}")

    pg_cur = pg_conn.cursor()
    # Use an E'' string so the NULL marker is reliably interpreted as a single backslash + N.
    copy_sql = (
        f"COPY {_quote_ident(table)} ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL E'\\\\N')"
    )

    total = None
    try:
        sqlite_count_cur = sqlite_conn.cursor()
        sqlite_count_cur.execute(f"SELECT COUNT(1) FROM {table}")
        total = int(sqlite_count_cur.fetchone()[0])
    except Exception:
        total = None

    imported = 0
    started = time.time()

    while True:
        rows = sqlite_cur.fetchmany(chunk_rows)
        if not rows:
            break

        sio = io.StringIO()
        writer = csv.writer(sio, lineterminator="\n")
        for row in rows:
            writer.writerow(["\\N" if v is None else v for v in row])
        sio.seek(0)

        pg_cur.copy_expert(copy_sql, sio)
        pg_conn.commit()

        imported += len(rows)
        if total:
            pct = imported / total * 100
            elapsed = time.time() - started
            speed = imported / elapsed if elapsed > 0 else 0
            print(f"[{table}] {imported}/{total} ({pct:.1f}%) rows, {speed:,.0f} rows/s")
        else:
            print(f"[{table}] {imported} rows")

    pg_cur.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import local SQLite stock_data.db into Postgres (Supabase)."
    )
    parser.add_argument("--sqlite-path", default="stock_data.db")
    parser.add_argument(
        "--database-url",
        dest="database_url",
        default=None,
        help="Postgres connection URI. Recommended to set env var DATABASE_URL.",
    )
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=50_000,
        help="Rows per COPY chunk (default: 50000).",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="TRUNCATE target tables before import (destructive).",
    )
    parser.add_argument(
        "--no-trades",
        action="store_true",
        help="Skip trade_* tables (only import market data).",
    )
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        print(f"ERROR: SQLite file not found: {sqlite_path}")
        return 2

    if args.database_url:
        os.environ["DATABASE_URL"] = args.database_url.strip()

    # Ensure repo root is on sys.path (avoid importing third-party package named `db`).
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    import db

    database_url = db.get_database_url()
    if not database_url:
        try:
            database_url = input("Paste DATABASE_URL (will not be saved): ").strip()
        except EOFError:
            database_url = None
        if database_url:
            os.environ["DATABASE_URL"] = database_url

    if not database_url:
        print("ERROR: Missing DATABASE_URL.")
        return 2

    if db.get_backend() != "postgres":
        print("ERROR: DATABASE_URL does not look like a Postgres URI.")
        return 2

    # Ensure schema exists
    import data_loader
    import trader

    data_loader.init_tables()
    trader.init_trade_system(reset=False)

    sqlite_conn = sqlite3.connect(str(sqlite_path), timeout=30)
    try:
        pg_conn = db.get_db_connection()
    except Exception as exc:
        print(f"ERROR: Failed to connect Postgres: {exc}")
        sqlite_conn.close()
        return 2

    pg_conn.autocommit = False
    pg_cur = pg_conn.cursor()

    # Speed-up knobs (best-effort; some may be restricted)
    for stmt in [
        "SET statement_timeout TO 0",
        "SET lock_timeout TO 0",
        "SET idle_in_transaction_session_timeout TO 0",
        "SET synchronous_commit TO OFF",
    ]:
        try:
            pg_cur.execute(stmt)
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()

    selected = TABLES
    if args.no_trades:
        selected = [t for t in TABLES if not t["name"].startswith("trade_")]

    non_empty = []
    if not args.truncate:
        for t in selected:
            try:
                if _table_has_rows(pg_cur, t["name"]):
                    non_empty.append(t["name"])
            except Exception as exc:
                print(f"ERROR: Failed to inspect table {t['name']}: {exc}")
                pg_conn.close()
                sqlite_conn.close()
                return 2
        if non_empty:
            print("ERROR: Target tables are not empty:")
            for name in non_empty:
                print(f"  - {name}")
            print("Re-run with --truncate if you want to overwrite them.")
            pg_conn.close()
            sqlite_conn.close()
            return 2

    if args.truncate:
        print("Truncating target tables...")
        names_sql = ", ".join(_quote_ident(t["name"]) for t in selected)
        pg_cur.execute(f"TRUNCATE TABLE {names_sql}")
        pg_conn.commit()

    print("Dropping optional indexes for faster import...")
    _drop_optional_indexes(pg_cur)
    pg_conn.commit()

    for t in selected:
        name = t["name"]
        cols = t["columns"]
        print(f"Importing table: {name}")
        try:
            _copy_table(
                sqlite_conn=sqlite_conn,
                pg_conn=pg_conn,
                table=name,
                columns=cols,
                chunk_rows=args.chunk_rows,
            )
        except Exception as exc:
            pg_conn.rollback()
            print(f"ERROR: Import failed for table {name}: {exc}")
            pg_conn.close()
            sqlite_conn.close()
            return 1

        post = t.get("post_import")
        if post == "fix_trade_orders_sequence":
            try:
                _fix_trade_orders_sequence(pg_cur)
                pg_conn.commit()
            except Exception:
                pg_conn.rollback()

    print("Recreating optional indexes...")
    _create_optional_indexes(pg_cur)
    pg_conn.commit()

    pg_conn.close()
    sqlite_conn.close()
    print("OK: Import completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
