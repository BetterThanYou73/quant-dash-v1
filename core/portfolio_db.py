"""
Portfolio persistence layer.

Stores user portfolios + positions in Postgres on Heroku, in a local
SQLite file when running locally without DATABASE_URL.

Why this module exists separately from `data_engine`:
  data_engine owns the *market* data cache (one giant pickled DataFrame).
  This module owns *user* data (small, transactional, per-row updates).
  They have different shapes and different durability requirements:
   - market data is regenerable from yfinance \u2014 losing it is annoying.
   - user portfolios are NOT regenerable \u2014 losing them is a P0 incident.

Identity model (Phase 2a only):
  Phase 2a uses an opaque `device_id` cookie as the owner key. The user
  doesn't sign up; their browser gets a UUID that scopes all reads/writes.
  This lets us ship the Postgres-backed UI today without building auth.

  Phase 2b will add a `users` table and a one-shot migration: when a
  device_id signs in for the first time, its rows are reassigned to the
  authenticated user_id. The schema is designed so that migration is a
  single UPDATE statement.

Schema:
    portfolios (
      id            BIGSERIAL PK,
      owner_kind    TEXT NOT NULL,           -- 'device' | 'user' (post 2b)
      owner_id      TEXT NOT NULL,           -- the device uuid or user id
      name          TEXT NOT NULL DEFAULT 'Main',
      created_at    TIMESTAMPTZ DEFAULT now(),
      UNIQUE (owner_kind, owner_id, name)
    )
    positions (
      id            BIGSERIAL PK,
      portfolio_id  BIGINT REFERENCES portfolios(id) ON DELETE CASCADE,
      ticker        TEXT NOT NULL,
      shares        NUMERIC(20,6) NOT NULL,
      avg_cost      NUMERIC(20,6) NOT NULL,
      opened_at     TIMESTAMPTZ DEFAULT now(),
      UNIQUE (portfolio_id, ticker)
    )

Concurrency: every read/write opens its own connection from the Heroku
20-conn pool, runs in <50 ms, and closes. No long-lived sessions.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from core import data_engine as de


# ---- backend selection ---------------------------------------------------

_LOCAL_SQLITE_PATH = Path(__file__).resolve().parents[1] / "cache" / "portfolio.sqlite3"
_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False


def _backend() -> str:
    """Return 'postgres' if we have DATABASE_URL+psycopg, else 'sqlite'.

    We piggy-back on data_engine's detection so a single CACHE_BACKEND
    or DATABASE_URL env var controls everything.
    """
    return "postgres" if de._cache_backend() == "postgres" else "sqlite"


@contextmanager
def _conn():
    """Yield a connection appropriate for the active backend.

    Always closes on exit. Always commits on successful exit; rolls back
    on exception. Treat each `with _conn() as c:` block as one transaction.
    """
    backend = _backend()
    if backend == "postgres":
        c = de._pg_conn()
        if c is None:
            # DATABASE_URL set but psycopg missing \u2014 fall through to sqlite
            # so local dev still works.
            backend = "sqlite"
        else:
            try:
                yield c
                c.commit()
            except Exception:
                c.rollback()
                raise
            finally:
                c.close()
            return

    # sqlite path
    _LOCAL_SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(_LOCAL_SQLITE_PATH))
    c.execute("PRAGMA foreign_keys = ON")
    try:
        yield c
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def _placeholder() -> str:
    """psycopg uses %s, sqlite3 uses ?. Return the right one."""
    return "%s" if _backend() == "postgres" else "?"


def _serial() -> str:
    """Auto-increment PK type per backend."""
    return "BIGSERIAL" if _backend() == "postgres" else "INTEGER PRIMARY KEY AUTOINCREMENT"


def _ts_default() -> str:
    return "TIMESTAMPTZ DEFAULT now()" if _backend() == "postgres" else "TEXT DEFAULT (datetime('now'))"


def ensure_schema() -> None:
    """Create tables on first use. Idempotent and cached after first
    success so we don't run CREATE TABLE on every request."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        with _conn() as c:
            cur = c.cursor()
            if _backend() == "postgres":
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolios (
                        id          BIGSERIAL PRIMARY KEY,
                        owner_kind  TEXT NOT NULL,
                        owner_id    TEXT NOT NULL,
                        name        TEXT NOT NULL DEFAULT 'Main',
                        created_at  TIMESTAMPTZ DEFAULT now(),
                        UNIQUE (owner_kind, owner_id, name)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS positions (
                        id            BIGSERIAL PRIMARY KEY,
                        portfolio_id  BIGINT NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
                        ticker        TEXT NOT NULL,
                        shares        NUMERIC(20,6) NOT NULL CHECK (shares > 0),
                        avg_cost      NUMERIC(20,6) NOT NULL CHECK (avg_cost >= 0),
                        opened_at     TIMESTAMPTZ DEFAULT now(),
                        UNIQUE (portfolio_id, ticker)
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_positions_portfolio ON positions(portfolio_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_portfolios_owner ON portfolios(owner_kind, owner_id)")
            else:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolios (
                        id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        owner_kind  TEXT NOT NULL,
                        owner_id    TEXT NOT NULL,
                        name        TEXT NOT NULL DEFAULT 'Main',
                        created_at  TEXT DEFAULT (datetime('now')),
                        UNIQUE (owner_kind, owner_id, name)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS positions (
                        id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        portfolio_id  INTEGER NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
                        ticker        TEXT NOT NULL,
                        shares        REAL NOT NULL CHECK (shares > 0),
                        avg_cost      REAL NOT NULL CHECK (avg_cost >= 0),
                        opened_at     TEXT DEFAULT (datetime('now')),
                        UNIQUE (portfolio_id, ticker)
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_positions_portfolio ON positions(portfolio_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_portfolios_owner ON portfolios(owner_kind, owner_id)")
        _SCHEMA_READY = True


# ---- portfolio operations -----------------------------------------------

def get_or_create_main_portfolio(owner_kind: str, owner_id: str) -> int:
    """Return the portfolio id for (kind, id), creating it if needed.

    Every owner gets one auto-created 'Main' portfolio on first interaction.
    Multi-portfolio support is a Phase 2b feature.
    """
    ensure_schema()
    ph = _placeholder()
    with _conn() as c:
        cur = c.cursor()
        cur.execute(
            f"SELECT id FROM portfolios WHERE owner_kind={ph} AND owner_id={ph} AND name='Main'",
            (owner_kind, owner_id),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])
        cur.execute(
            f"INSERT INTO portfolios (owner_kind, owner_id, name) VALUES ({ph}, {ph}, 'Main') "
            f"{'RETURNING id' if _backend() == 'postgres' else ''}",
            (owner_kind, owner_id),
        )
        if _backend() == "postgres":
            return int(cur.fetchone()[0])
        return int(cur.lastrowid)


def list_positions(owner_kind: str, owner_id: str) -> list[dict[str, Any]]:
    """Return all positions for the owner's Main portfolio, newest first."""
    ensure_schema()
    pid = get_or_create_main_portfolio(owner_kind, owner_id)
    ph = _placeholder()
    with _conn() as c:
        cur = c.cursor()
        cur.execute(
            f"SELECT ticker, shares, avg_cost, opened_at FROM positions "
            f"WHERE portfolio_id={ph} ORDER BY opened_at DESC, id DESC",
            (pid,),
        )
        rows = cur.fetchall()
    return [
        {
            "ticker": r[0],
            "shares": float(r[1]),
            "avg_cost": float(r[2]),
            "opened_at": str(r[3]) if r[3] is not None else None,
        }
        for r in rows
    ]


def upsert_position(owner_kind: str, owner_id: str, ticker: str, shares: float, avg_cost: float) -> dict[str, Any]:
    """Add a new position OR average into an existing one.

    Cost-basis math when a position already exists:
        new_avg_cost = (old_shares * old_avg + new_shares * new_avg) / total_shares
    Same weighted-average rule the localStorage frontend used \u2014 the
    server is now authoritative.
    """
    ensure_schema()
    if shares <= 0 or avg_cost < 0:
        raise ValueError("shares must be > 0 and avg_cost must be >= 0")
    sym = ticker.strip().upper()
    if not sym:
        raise ValueError("ticker is required")

    pid = get_or_create_main_portfolio(owner_kind, owner_id)
    ph = _placeholder()
    with _conn() as c:
        cur = c.cursor()
        cur.execute(
            f"SELECT shares, avg_cost FROM positions WHERE portfolio_id={ph} AND ticker={ph}",
            (pid, sym),
        )
        existing = cur.fetchone()
        if existing:
            old_shares, old_avg = float(existing[0]), float(existing[1])
            total = old_shares + shares
            blended = (old_shares * old_avg + shares * avg_cost) / total if total > 0 else avg_cost
            cur.execute(
                f"UPDATE positions SET shares={ph}, avg_cost={ph} "
                f"WHERE portfolio_id={ph} AND ticker={ph}",
                (total, blended, pid, sym),
            )
            return {"ticker": sym, "shares": total, "avg_cost": blended, "merged": True}

        cur.execute(
            f"INSERT INTO positions (portfolio_id, ticker, shares, avg_cost) "
            f"VALUES ({ph}, {ph}, {ph}, {ph})",
            (pid, sym, shares, avg_cost),
        )
        return {"ticker": sym, "shares": shares, "avg_cost": avg_cost, "merged": False}


def delete_position(owner_kind: str, owner_id: str, ticker: str) -> bool:
    """Remove a position. Returns True if a row was deleted."""
    ensure_schema()
    sym = ticker.strip().upper()
    pid = get_or_create_main_portfolio(owner_kind, owner_id)
    ph = _placeholder()
    with _conn() as c:
        cur = c.cursor()
        cur.execute(
            f"DELETE FROM positions WHERE portfolio_id={ph} AND ticker={ph}",
            (pid, sym),
        )
        return (cur.rowcount or 0) > 0


def replace_all_positions(owner_kind: str, owner_id: str, items: list[dict[str, Any]]) -> int:
    """Atomically replace every position in the owner's Main portfolio.

    Used when the frontend bulk-imports from localStorage on first login,
    or when the user does a full edit/save in the portfolio modal.
    Returns the count of rows written.
    """
    ensure_schema()
    pid = get_or_create_main_portfolio(owner_kind, owner_id)
    ph = _placeholder()
    cleaned: list[tuple[str, float, float]] = []
    for it in items or []:
        sym = (it.get("ticker") or "").strip().upper()
        try:
            shares = float(it.get("shares"))
            avg_cost = float(it.get("avg_cost"))
        except (TypeError, ValueError):
            continue
        if not sym or shares <= 0 or avg_cost < 0:
            continue
        cleaned.append((sym, shares, avg_cost))

    with _conn() as c:
        cur = c.cursor()
        cur.execute(f"DELETE FROM positions WHERE portfolio_id={ph}", (pid,))
        for sym, shares, avg_cost in cleaned:
            cur.execute(
                f"INSERT INTO positions (portfolio_id, ticker, shares, avg_cost) "
                f"VALUES ({ph}, {ph}, {ph}, {ph})",
                (pid, sym, shares, avg_cost),
            )
    return len(cleaned)
