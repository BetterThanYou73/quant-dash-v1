"""
User authentication persistence layer.

Stores user accounts (email + bcrypt password hash) alongside the
portfolio tables. Uses the same dual-backend (Postgres on Heroku,
SQLite locally) pattern as `portfolio_db`.

Why a separate module:
    portfolio_db owns *user data* tables (portfolios + positions).
    users_db owns the *identity* table only. Keeping them apart means
    the auth layer can be tested in isolation, and a future migration
    to a managed identity provider (e.g. Auth0) only touches one file.

Schema:
    users (
      id             BIGSERIAL PK,
      email          TEXT NOT NULL UNIQUE,    -- lowercased before insert
      password_hash  TEXT NOT NULL,           -- bcrypt
      created_at     TIMESTAMPTZ DEFAULT now()
    )

Identity migration (called from /api/auth/login):
    UPDATE portfolios SET owner_kind='user', owner_id=:user_id
    WHERE  owner_kind='device' AND owner_id=:device_id
    AND NOT EXISTS (
        SELECT 1 FROM portfolios WHERE owner_kind='user' AND owner_id=:user_id
    );
The NOT EXISTS guard means a user who already has portfolios won't have
them clobbered by re-signing-in from a fresh browser. The first-device-
wins rule is intentional: it matches the user's expectation that the
portfolio they were just looking at follows them into their account.
"""

from __future__ import annotations

import re
import threading
from typing import Optional

import bcrypt

from core import portfolio_db as pdb  # reuse _conn / _placeholder / _backend


_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False

# RFC 5322 is overkill — this regex catches obvious garbage and that's
# enough. Real verification happens via "send a confirmation email"
# which is post-MVP.
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

# bcrypt cost. 12 rounds ≈ 250 ms per hash on a modern CPU. Acceptable
# given login is rare; signup throughput is not a bottleneck.
_BCRYPT_ROUNDS = 12


def ensure_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        # Make sure portfolios + positions exist too — login migration
        # touches them and we don't want a chicken-and-egg failure.
        pdb.ensure_schema()
        with pdb._conn() as c:
            cur = c.cursor()
            if pdb._backend() == "postgres":
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id             BIGSERIAL PRIMARY KEY,
                        email          TEXT NOT NULL UNIQUE,
                        password_hash  TEXT NOT NULL,
                        created_at     TIMESTAMPTZ DEFAULT now()
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(LOWER(email))")
            else:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id             INTEGER PRIMARY KEY AUTOINCREMENT,
                        email          TEXT NOT NULL UNIQUE,
                        password_hash  TEXT NOT NULL,
                        created_at     TEXT DEFAULT (datetime('now'))
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        _SCHEMA_READY = True


def normalize_email(email: str) -> str:
    """Lowercase + strip. Validation is `is_valid_email()`."""
    return (email or "").strip().lower()


def is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email or ""))


def is_valid_password(pw: str) -> Optional[str]:
    """Return None if valid, otherwise an error string."""
    if not pw or len(pw) < 8:
        return "Password must be at least 8 characters."
    if len(pw) > 128:
        return "Password too long (max 128)."
    return None


def hash_password(pw: str) -> str:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt(rounds=_BCRYPT_ROUNDS)).decode("ascii")


def verify_password(pw: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(pw.encode("utf-8"), hashed.encode("ascii"))
    except (ValueError, TypeError):
        return False


# ---- CRUD ---------------------------------------------------------------

def create_user(email: str, password: str) -> dict:
    """Returns {id, email, created_at}. Raises ValueError on duplicate
    email or invalid input."""
    ensure_schema()
    norm = normalize_email(email)
    if not is_valid_email(norm):
        raise ValueError("invalid email")
    pw_err = is_valid_password(password)
    if pw_err:
        raise ValueError(pw_err)

    pw_hash = hash_password(password)
    ph = pdb._placeholder()

    with pdb._conn() as c:
        cur = c.cursor()
        # Check for duplicate first so we can return a clean error.
        cur.execute(f"SELECT id FROM users WHERE email = {ph}", (norm,))
        if cur.fetchone():
            raise ValueError("email already registered")

        if pdb._backend() == "postgres":
            cur.execute(
                f"INSERT INTO users (email, password_hash) VALUES ({ph}, {ph}) RETURNING id, created_at",
                (norm, pw_hash),
            )
            row = cur.fetchone()
            uid, created = row[0], row[1]
        else:
            cur.execute(
                f"INSERT INTO users (email, password_hash) VALUES ({ph}, {ph})",
                (norm, pw_hash),
            )
            uid = cur.lastrowid
            cur.execute(f"SELECT created_at FROM users WHERE id = {ph}", (uid,))
            created = cur.fetchone()[0]
        return {"id": int(uid), "email": norm, "created_at": str(created)}


def find_user_by_email(email: str) -> Optional[dict]:
    """Returns {id, email, password_hash, created_at} or None."""
    ensure_schema()
    norm = normalize_email(email)
    if not norm:
        return None
    ph = pdb._placeholder()
    with pdb._conn() as c:
        cur = c.cursor()
        cur.execute(
            f"SELECT id, email, password_hash, created_at FROM users WHERE email = {ph}",
            (norm,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": int(row[0]),
            "email": row[1],
            "password_hash": row[2],
            "created_at": str(row[3]),
        }


def find_user_by_id(user_id: int) -> Optional[dict]:
    ensure_schema()
    ph = pdb._placeholder()
    with pdb._conn() as c:
        cur = c.cursor()
        cur.execute(
            f"SELECT id, email, created_at FROM users WHERE id = {ph}",
            (int(user_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"id": int(row[0]), "email": row[1], "created_at": str(row[2])}


# ---- device → user portfolio migration ----------------------------------

def migrate_device_portfolios_to_user(device_id: str, user_id: int) -> int:
    """Reassign every portfolio owned by `device_id` to `user_id`.

    Idempotent and safe under repeat calls. The NOT EXISTS guard means
    we don't overwrite an existing user's portfolios — a user who signed
    in from a fresh device gets to keep what's already in their account.

    Returns the number of portfolio rows reassigned.
    """
    if not device_id:
        return 0
    ensure_schema()
    ph = pdb._placeholder()
    with pdb._conn() as c:
        cur = c.cursor()
        # Skip migration if the user already has portfolios — protects
        # existing data from being silently overwritten.
        cur.execute(
            f"SELECT 1 FROM portfolios WHERE owner_kind = 'user' AND owner_id = {ph} LIMIT 1",
            (str(user_id),),
        )
        if cur.fetchone():
            return 0
        cur.execute(
            f"""UPDATE portfolios
                SET    owner_kind = 'user', owner_id = {ph}
                WHERE  owner_kind = 'device' AND owner_id = {ph}""",
            (str(user_id), device_id),
        )
        return cur.rowcount or 0
