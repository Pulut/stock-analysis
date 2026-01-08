import os
import sys
import argparse
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize schema in Supabase Postgres.")
    parser.add_argument(
        "--database-url",
        dest="database_url",
        default=None,
        help="Supabase Postgres connection URI (recommended: use env var DATABASE_URL).",
    )
    parser.add_argument(
        "--reset-trades",
        action="store_true",
        help="Drop and recreate trade_* tables (destructive).",
    )
    args = parser.parse_args()

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
        print("ERROR: Missing Supabase Postgres connection string.")
        print("Provide it via one of:")
        print("  1) env var DATABASE_URL")
        print("  2) --database-url")
        print("Example (PowerShell):")
        print('  $env:DATABASE_URL="postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require"')
        return 2

    backend = db.get_backend()
    if backend != "postgres":
        print(f"ERROR: Expected Postgres backend, got: {backend}")
        return 2

    import data_loader
    import trader

    data_loader.init_tables()
    trader.init_trade_system(reset=bool(args.reset_trades))

    print("OK: Schema initialized in Postgres.")
    if args.reset_trades:
        print("NOTE: trade_* tables were reset.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
