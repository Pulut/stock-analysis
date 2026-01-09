import os
import sqlite3
from typing import Any, Optional, Literal
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

SQLITE_DB_PATH = os.environ.get("SQLITE_DB_PATH", "stock_data.db")

Backend = Literal["sqlite", "postgres"]


def _get_database_url_from_streamlit_secrets() -> Optional[str]:
    try:
        import streamlit as st

        if "DATABASE_URL" in st.secrets:
            return str(st.secrets["DATABASE_URL"])
        if "postgres" in st.secrets and "url" in st.secrets["postgres"]:
            return str(st.secrets["postgres"]["url"])
    except Exception:
        return None
    return None


def get_database_url() -> Optional[str]:
    return (
        os.environ.get("DATABASE_URL")
        or os.environ.get("POSTGRES_URL")
        or _get_database_url_from_streamlit_secrets()
    )


def get_backend() -> Backend:
    url = get_database_url()
    if url and (url.startswith("postgres://") or url.startswith("postgresql://")):
        return "postgres"
    return "sqlite"


def _normalize_postgres_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]

    parsed = urlparse(url)
    if parsed.scheme != "postgresql":
        return url

    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "sslmode" not in query:
        query["sslmode"] = "require"
        parsed = parsed._replace(query=urlencode(query))
        url = urlunparse(parsed)
    return url


def describe_database() -> str:
    """
    Returns a safe, human-readable description of the active DB target.
    (Never includes credentials.)
    """
    backend = get_backend()
    if backend == "sqlite":
        return f"sqlite:///{SQLITE_DB_PATH}"

    url = get_database_url()
    if not url:
        return "postgres"
    try:
        url = _normalize_postgres_url(url)
        parsed = urlparse(url)
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        dbname = (parsed.path or "").lstrip("/") or ""
        if host or dbname:
            return f"postgres://{host}{port}/{dbname}"
    except Exception:
        pass
    return "postgres"


def get_db_connection():
    backend = get_backend()
    if backend == "postgres":
        url = get_database_url()
        if not url:
            raise RuntimeError("DATABASE_URL is required for Postgres backend.")
        url = _normalize_postgres_url(url)
        try:
            import psycopg2
        except Exception as exc:
            raise RuntimeError(
                "Postgres backend requires psycopg2-binary. Add it to requirements.txt."
            ) from exc
        return psycopg2.connect(url, connect_timeout=30)

    conn = sqlite3.connect(SQLITE_DB_PATH, timeout=30)
    # Enable WAL mode for better concurrency (Writer doesn't block Readers)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def adapt_sql(sql: str, backend: Optional[Backend] = None) -> str:
    if backend is None:
        backend = get_backend()
    if backend == "postgres":
        return sql.replace("?", "%s")
    return sql


class CursorAdapter:
    def __init__(self, cursor: Any, backend: Backend):
        self._cursor = cursor
        self._backend = backend

    def execute(self, sql: str, params: Any = None):
        sql = adapt_sql(sql, self._backend)
        if params is None:
            return self._cursor.execute(sql)
        return self._cursor.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Any):
        sql = adapt_sql(sql, self._backend)
        return self._cursor.executemany(sql, seq_of_params)

    def __getattr__(self, item: str):
        return getattr(self._cursor, item)


def get_cursor(conn) -> CursorAdapter:
    backend = get_backend()
    return CursorAdapter(conn.cursor(), backend)
