"""Acceso a PostgreSQL: pool de conexiones, transacciones con reintento y migraciones."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TypeVar

from psycopg import Cursor, errors
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import SQL_DIR

log = logging.getLogger(__name__)
T = TypeVar("T")

# Errores en los que repetir la transacción completa es seguro.
RETRYABLE = (errors.DeadlockDetected, errors.SerializationFailure, errors.LockNotAvailable)

MIGRATION_LOCK = 7_000_001  # pg_advisory_lock para que solo una instancia migre

# Nombre con el que se identifican las conexiones en pg_stat_activity.
SERVICE_APP_NAME = "guardian-service"
CLI_APP_NAME = "guardian-cli"

BASE_SCHEMA = "001_schema.sql"
BASE_TABLES = ("processed_events", "pending_events", "outbox", "parts", "locations", "models", "bom_lines",
               "stock_policies", "work_orders", "inspections", "needs", "movements", "reservations",
               "shortages", "unmatched_receipts", "reorder_suggestions")

# Borra vistas, tablas y funciones de `public` que pertenecen al usuario del servicio. No
# necesita ser dueño de la base ni del esquema (a diferencia de DROP SCHEMA public).
DROP_OWN_OBJECTS = """
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT viewname FROM pg_views WHERE schemaname = 'public' AND viewowner = current_user LOOP
        EXECUTE format('DROP VIEW IF EXISTS public.%I CASCADE', r.viewname);
    END LOOP;
    FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tableowner = current_user LOOP
        EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', r.tablename);
    END LOOP;
    FOR r IN SELECT p.oid::regprocedure AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = 'public' AND pg_get_userbyid(p.proowner) = current_user LOOP
        EXECUTE format('DROP FUNCTION IF EXISTS %s CASCADE', r.fn);
    END LOOP;
END $$;
"""


class Database:
    def __init__(self, url: str, max_size: int = 10, application_name: str = CLI_APP_NAME) -> None:
        self.pool = ConnectionPool(
            url,
            min_size=1,
            max_size=max_size,
            kwargs={"row_factory": dict_row, "autocommit": False, "application_name": application_name},
            open=False,
        )

    def open(self, timeout: float = 30) -> None:
        self.pool.open(wait=True, timeout=timeout)

    def close(self) -> None:
        self.pool.close()

    @contextmanager
    def transaction(self) -> Iterator[Cursor]:
        """Una transacción: commit al salir, rollback si hay excepción."""
        with self.pool.connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    yield cur

    def run(self, fn: Callable[[Cursor], T], *, retries: int = 5) -> T:
        """Ejecuta `fn` en una transacción y la repite ante deadlocks o fallas de serialización."""
        for attempt in range(1, retries + 1):
            try:
                with self.transaction() as cur:
                    return fn(cur)
            except RETRYABLE as exc:
                if attempt == retries:
                    raise
                log.warning("transacción reintentada (%s/%s): %s", attempt, retries, exc)
                time.sleep(0.05 * attempt)
        raise AssertionError("inalcanzable")

    # --- esquema ---------------------------------------------------------------
    def migrate(self) -> list[str]:
        """Aplica en orden los archivos sql/NNN_*.sql que falten. Devuelve los aplicados."""
        applied: list[str] = []
        with self.pool.connection() as conn:
            conn.autocommit = True
            conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK,))
            try:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS schema_migrations ("
                    " version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
                )
                done = {r["version"] for r in conn.execute("SELECT version FROM schema_migrations")}
                if not done and self._base_schema_present(conn):
                    # El esquema base (001) se aplicó a mano antes de usar el migrador: se adopta.
                    log.info("esquema base encontrado sin registro de migraciones: se adopta %s", BASE_SCHEMA)
                    conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (BASE_SCHEMA,))
                    done.add(BASE_SCHEMA)
                for path in sorted(SQL_DIR.glob("[0-9][0-9][0-9]_*.sql")):
                    if path.name in done:
                        continue
                    log.info("aplicando migración %s", path.name)
                    with conn.transaction():
                        conn.execute(path.read_text(encoding="utf-8"))
                        conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,))
                    applied.append(path.name)
            finally:
                conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK,))
                conn.autocommit = False
        return applied

    @staticmethod
    def _base_schema_present(conn) -> bool:
        row = conn.execute(
            "SELECT count(*) AS n FROM information_schema.tables"
            " WHERE table_schema = 'public' AND table_name = ANY(%s)",
            (list(BASE_TABLES),),
        ).fetchone()
        return row["n"] == len(BASE_TABLES)

    def reset(self) -> None:
        """Borra todo (incluida la posición de lectura) y vuelve a crear el esquema.

        Se niega si el servicio sigue conectado: al apagarse podría escribir su posición de
        lectura vieja en la base nueva y la reconstrucción se saltaría eventos.
        """
        with self.pool.connection() as conn:
            running = conn.execute(
                "SELECT count(*) AS n FROM pg_stat_activity"
                " WHERE datname = current_database() AND application_name = %s",
                (SERVICE_APP_NAME,),
            ).fetchone()["n"]
            if running:
                raise RuntimeError(f"hay {running} conexiones del servicio abiertas: deténganlo antes de reset-db")
            with conn.transaction():
                conn.execute(DROP_OWN_OBJECTS)
        self.migrate()
