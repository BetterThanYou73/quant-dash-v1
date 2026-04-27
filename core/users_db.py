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
                        display_name   TEXT,
                        created_at     TIMESTAMPTZ DEFAULT now()
                    )
                """)
                # Idempotent migration for existing deploys.
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_digest_enabled BOOLEAN DEFAULT FALSE")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_digest_include_ai BOOLEAN DEFAULT TRUE")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_digest_last_sent TIMESTAMPTZ")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(LOWER(email))")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_digest ON users(email_digest_enabled) WHERE email_digest_enabled = TRUE")
            else:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id             INTEGER PRIMARY KEY AUTOINCREMENT,
                        email          TEXT NOT NULL UNIQUE,
                        password_hash  TEXT NOT NULL,
                        display_name   TEXT,
                        created_at     TEXT DEFAULT (datetime('now'))
                    )
                """)
                # SQLite: ALTER TABLE ... ADD COLUMN tolerates missing columns
                # but errors if it already exists, so guard with PRAGMA.
                cur.execute("PRAGMA table_info(users)")
                cols = {row[1] for row in cur.fetchall()}
                if "display_name" not in cols:
                    cur.execute("ALTER TABLE users ADD COLUMN display_name TEXT")
                if "email_digest_enabled" not in cols:
                    cur.execute("ALTER TABLE users ADD COLUMN email_digest_enabled INTEGER DEFAULT 0")
                if "email_digest_include_ai" not in cols:
                    cur.execute("ALTER TABLE users ADD COLUMN email_digest_include_ai INTEGER DEFAULT 1")
                if "email_digest_last_sent" not in cols:
                    cur.execute("ALTER TABLE users ADD COLUMN email_digest_last_sent TEXT")
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

def create_user(email: str, password: str, display_name: Optional[str] = None) -> dict:
    """Returns {id, email, display_name, created_at}. Raises ValueError on
    duplicate email or invalid input."""
    ensure_schema()
    norm = normalize_email(email)
    if not is_valid_email(norm):
        raise ValueError("invalid email")
    pw_err = is_valid_password(password)
    if pw_err:
        raise ValueError(pw_err)

    # display_name is optional; trim and cap so a malicious client can't
    # pad the column.
    name = (display_name or "").strip() or None
    if name and len(name) > 80:
        name = name[:80]

    pw_hash = hash_password(password)
    ph = pdb._placeholder()

    with pdb._conn() as c:
        cur = c.cursor()
        cur.execute(f"SELECT id FROM users WHERE email = {ph}", (norm,))
        if cur.fetchone():
            raise ValueError("email already registered")

        if pdb._backend() == "postgres":
            cur.execute(
                f"INSERT INTO users (email, password_hash, display_name) VALUES ({ph}, {ph}, {ph}) RETURNING id, created_at",
                (norm, pw_hash, name),
            )
            row = cur.fetchone()
            uid, created = row[0], row[1]
        else:
            cur.execute(
                f"INSERT INTO users (email, password_hash, display_name) VALUES ({ph}, {ph}, {ph})",
                (norm, pw_hash, name),
            )
            uid = cur.lastrowid
            cur.execute(f"SELECT created_at FROM users WHERE id = {ph}", (uid,))
            created = cur.fetchone()[0]
        return {"id": int(uid), "email": norm, "display_name": name, "created_at": str(created)}


def find_user_by_email(email: str) -> Optional[dict]:
    """Returns {id, email, password_hash, display_name, created_at} or None."""
    ensure_schema()
    norm = normalize_email(email)
    if not norm:
        return None
    ph = pdb._placeholder()
    with pdb._conn() as c:
        cur = c.cursor()
        cur.execute(
            f"SELECT id, email, password_hash, display_name, created_at FROM users WHERE email = {ph}",
            (norm,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": int(row[0]),
            "email": row[1],
            "password_hash": row[2],
            "display_name": row[3],
            "created_at": str(row[4]),
        }


def find_user_by_id(user_id: int) -> Optional[dict]:
    ensure_schema()
    ph = pdb._placeholder()
    with pdb._conn() as c:
        cur = c.cursor()
        cur.execute(
            f"SELECT id, email, display_name, created_at FROM users WHERE id = {ph}",
            (int(user_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"id": int(row[0]), "email": row[1], "display_name": row[2], "created_at": str(row[3])}


def update_display_name(user_id: int, name: Optional[str]) -> Optional[dict]:
    """Set or clear the display_name. Returns the updated user dict, or None."""
    ensure_schema()
    clean = (name or "").strip() or None
    if clean and len(clean) > 80:
        clean = clean[:80]
    ph = pdb._placeholder()
    with pdb._conn() as c:
        cur = c.cursor()
        cur.execute(
            f"UPDATE users SET display_name = {ph} WHERE id = {ph}",
            (clean, int(user_id)),
        )
        c.commit()
    return find_user_by_id(user_id)


# ---- digest prefs --------------------------------------------------------
# Stored on the users row directly (cheap, single read with login).
# `enabled`: master opt-in. `include_ai`: spend their BYOK Anthropic key
# on a daily commentary section. `last_sent`: cron uses this for dedupe
# so a re-run within the same UTC day doesn't double-mail.

def get_digest_prefs(user_id: int) -> dict:
    """Return {enabled, include_ai, last_sent_utc}. Defaults if user not found."""
    ensure_schema()
    ph = pdb._placeholder()
    with pdb._conn() as c:
        cur = c.cursor()
        cur.execute(
            f"SELECT email_digest_enabled, email_digest_include_ai, email_digest_last_sent "
            f"FROM users WHERE id = {ph}",
            (int(user_id),),
        )
        row = cur.fetchone()
    if not row:
        return {"enabled": False, "include_ai": True, "last_sent_utc": None}
    return {
        "enabled": bool(row[0]),
        "include_ai": bool(row[1]) if row[1] is not None else True,
        "last_sent_utc": str(row[2]) if row[2] else None,
    }


def set_digest_prefs(user_id: int, enabled: bool, include_ai: bool) -> dict:
    """Persist the user's digest preferences and return the new state."""
    ensure_schema()
    ph = pdb._placeholder()
    # SQLite stores bools as 0/1; pass python bool — both psycopg and
    # sqlite3 adapt it to the right type.
    with pdb._conn() as c:
        cur = c.cursor()
        cur.execute(
            f"UPDATE users SET email_digest_enabled = {ph}, email_digest_include_ai = {ph} "
            f"WHERE id = {ph}",
            (bool(enabled), bool(include_ai), int(user_id)),
        )
        c.commit()
    return get_digest_prefs(user_id)


def mark_digest_sent(user_id: int) -> None:
    """Stamp last_sent_utc = now(). Called after a successful send so the
    cron can skip users we've already mailed today."""
    ensure_schema()
    ph = pdb._placeholder()
    with pdb._conn() as c:
        cur = c.cursor()
        if pdb._backend() == "postgres":
            cur.execute(
                f"UPDATE users SET email_digest_last_sent = now() WHERE id = {ph}",
                (int(user_id),),
            )
        else:
            cur.execute(
                f"UPDATE users SET email_digest_last_sent = datetime('now') WHERE id = {ph}",
                (int(user_id),),
            )
        c.commit()


def list_digest_subscribers() -> list[dict]:
    """All users with email_digest_enabled = TRUE.

    Returns rows of {id, email, display_name, include_ai, last_sent_utc}.
    The cron uses this list to fan out daily mail.
    """
    ensure_schema()
    with pdb._conn() as c:
        cur = c.cursor()
        # No placeholder needed — boolean literal.
        cur.execute(
            "SELECT id, email, display_name, email_digest_include_ai, email_digest_last_sent "
            "FROM users WHERE email_digest_enabled = "
            + ("TRUE" if pdb._backend() == "postgres" else "1")
        )
        rows = cur.fetchall()
    return [
        {
            "id": int(r[0]),
            "email": r[1],
            "display_name": r[2],
            "include_ai": bool(r[3]) if r[3] is not None else True,
            "last_sent_utc": str(r[4]) if r[4] else None,
        }
        for r in rows
    ]


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
