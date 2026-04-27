"""
Per-user encrypted secret storage — used by the Advisor (Phase 2c) for
the BYOK (bring-your-own-key) pattern.

Why not store keys in plaintext:
    Even though the table is private and Heroku Postgres is encrypted at
    rest by AWS, a database snapshot or a leaked DATABASE_URL would
    expose every user's Anthropic key. Symmetric Fernet encryption with
    an app-side key (APP_ENCRYPTION_KEY env var) makes the DB blob
    useless on its own — an attacker needs BOTH the database row AND
    the Heroku config var to recover a key.

Threat model coverage:
    - Postgres backup leak                 → blocked (need APP_ENCRYPTION_KEY)
    - Read-only DATABASE_URL leak          → blocked (same)
    - Compromised dyno                     → keys recoverable in process
                                              memory; same as plaintext, but
                                              that's a much higher bar
    - Curious developer with DB shell      → blocked (Fernet token visible
                                              but not decryptable)
    - Stolen JWT session                   → caps blast radius via per-user
                                              rate limits in routes_advisor

Key rotation:
    1. heroku config:set APP_ENCRYPTION_KEY_NEW=$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')
    2. Run a one-shot re-encrypt script (TODO when needed)
    3. heroku config:set APP_ENCRYPTION_KEY=$APP_ENCRYPTION_KEY_NEW; unset APP_ENCRYPTION_KEY_NEW

Local dev:
    If APP_ENCRYPTION_KEY is unset we derive a deterministic key from
    SESSION_SECRET so local development works without extra env setup.
    NEVER rely on the fallback in production — always set the explicit
    var on Heroku.
"""

from __future__ import annotations

import base64
import hashlib
import os
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

from core import data_engine as de


# --- Cipher ---------------------------------------------------------------

_FERNET: Optional[Fernet] = None


def _derive_dev_key() -> bytes:
    """Deterministic 32-byte key from SESSION_SECRET for local dev only."""
    seed = (os.environ.get("SESSION_SECRET") or "dev-only-do-not-use-in-prod-quantdash-2026").encode()
    digest = hashlib.sha256(seed).digest()
    return base64.urlsafe_b64encode(digest)


def _cipher() -> Fernet:
    """Lazy singleton so tests can override APP_ENCRYPTION_KEY freely."""
    global _FERNET
    if _FERNET is not None:
        return _FERNET
    raw = os.environ.get("APP_ENCRYPTION_KEY")
    if raw:
        # User-supplied key. Must be a urlsafe base64-encoded 32 byte
        # string per Fernet spec.
        key = raw.encode() if isinstance(raw, str) else raw
    else:
        key = _derive_dev_key()
    _FERNET = Fernet(key)
    return _FERNET


def encrypt(plaintext: str) -> bytes:
    return _cipher().encrypt(plaintext.encode("utf-8"))


def decrypt(token: bytes) -> Optional[str]:
    """Returns plaintext or None if the token can't be decrypted (rotated
    key, corrupted blob). Never raises."""
    try:
        return _cipher().decrypt(token).decode("utf-8")
    except (InvalidToken, ValueError):
        return None


# --- Postgres-backed secret store ----------------------------------------

# Schema is created on first use. A user has at most one secret per
# provider — re-setting overwrites.
_TABLE = "user_api_keys"


def _ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_TABLE} (
                user_id     BIGINT NOT NULL,
                provider    TEXT   NOT NULL,
                ciphertext  BYTEA  NOT NULL,
                last4       TEXT   NOT NULL,
                created_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (user_id, provider)
            )
            """
        )
    conn.commit()


def set_user_key(user_id: int, provider: str, plaintext: str) -> dict:
    """Store/replace the user's key for a given provider.

    Returns a metadata dict (last4, updated_utc) suitable for echoing to
    the client. Never returns the plaintext or ciphertext.
    """
    if not plaintext or not plaintext.strip():
        raise ValueError("empty key")
    plaintext = plaintext.strip()
    last4 = plaintext[-4:] if len(plaintext) >= 4 else plaintext
    blob = encrypt(plaintext)

    conn = de._pg_conn()
    if conn is None:
        raise RuntimeError("database unavailable")
    try:
        _ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_TABLE} (user_id, provider, ciphertext, last4, created_utc, updated_utc)
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (user_id, provider) DO UPDATE SET
                    ciphertext  = EXCLUDED.ciphertext,
                    last4       = EXCLUDED.last4,
                    updated_utc = NOW()
                RETURNING last4, updated_utc
                """,
                (int(user_id), provider, blob, last4),
            )
            row = cur.fetchone()
        conn.commit()
        return {
            "provider": provider,
            "last4": row[0],
            "updated_utc": row[1].isoformat() if row[1] else None,
            "has_key": True,
        }
    finally:
        conn.close()


def get_user_key(user_id: int, provider: str) -> Optional[str]:
    """Return the decrypted plaintext key, or None if absent/undecryptable.

    This is the ONLY function that ever materializes the plaintext. Keep
    its callers minimal — pass the returned string straight into the
    SDK client and never log/echo it.
    """
    conn = de._pg_conn()
    if conn is None:
        return None
    try:
        _ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ciphertext FROM {_TABLE} WHERE user_id = %s AND provider = %s",
                (int(user_id), provider),
            )
            row = cur.fetchone()
        if not row:
            return None
        return decrypt(bytes(row[0]))
    finally:
        conn.close()


def get_user_key_status(user_id: int, provider: str) -> dict:
    """Metadata-only view: { has_key, last4, updated_utc }. Safe to send
    to the client — no plaintext ever leaves the server."""
    conn = de._pg_conn()
    if conn is None:
        return {"provider": provider, "has_key": False, "last4": None, "updated_utc": None}
    try:
        _ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT last4, updated_utc FROM {_TABLE} WHERE user_id = %s AND provider = %s",
                (int(user_id), provider),
            )
            row = cur.fetchone()
        if not row:
            return {"provider": provider, "has_key": False, "last4": None, "updated_utc": None}
        return {
            "provider": provider,
            "has_key": True,
            "last4": row[0],
            "updated_utc": row[1].isoformat() if row[1] else None,
        }
    finally:
        conn.close()


def delete_user_key(user_id: int, provider: str) -> bool:
    conn = de._pg_conn()
    if conn is None:
        return False
    try:
        _ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {_TABLE} WHERE user_id = %s AND provider = %s",
                (int(user_id), provider),
            )
            deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        conn.close()
