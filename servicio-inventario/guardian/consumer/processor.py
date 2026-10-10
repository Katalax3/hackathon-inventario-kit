"""Aplicación de un evento: idempotencia, orden por llave, encolado y DLQ.

Cada evento se aplica en UNA transacción que incluye:
  - el efecto en el inventario y los eventos de salida (outbox),
  - su registro en `processed_events` (regla 19),
  - la nueva posición de lectura en `consumer_offsets`.

Así, una caída a la mitad no deja nada a medias: al volver se relee desde la última
posición confirmada y lo ya aplicado se reconoce por su event_id.

Si el evento referencia algo que aún no llega, se guarda en `pending_events` y se
reintenta cuando llega un evento que puede destrabarlo (regla 20). Los eventos que llegan
después con la misma llave y tópico esperan detrás de él, para respetar el orden por llave.
Lo que no se puede procesar tras reintentar va a `inventory.dlq` sin detener el consumo
(regla 21).
"""

from __future__ import annotations

import enum
import json
import logging
import time
import uuid

import psycopg
from psycopg import Cursor
from psycopg.types.json import Jsonb

from .. import outbox
from ..config import DLQ_TOPIC, OUTPUT_TOPIC, Settings
from ..db import Database
from ..domain.finish import finish
from ..errors import InvalidEvent, MissingDependency
from ..timeutil import parse_iso
from ..uow import API_ORIGIN, UnitOfWork
from .handlers import INPUT_HANDLERS, JOURNAL_HANDLERS, provides
from .validation import ContractValidator

log = logging.getLogger(__name__)

Position = tuple[str, int, int]  # (tópico, partición, siguiente offset)


class Outcome(enum.Enum):
    APPLIED = "aplicado"
    DUPLICATE = "duplicado"
    PARKED = "en espera"
    BLOCKED = "detrás de otro en espera"
    IGNORED = "ignorado"
    DEAD = "a la DLQ"


def save_offset(cur: Cursor, position: Position | None) -> None:
    if position is None:
        return
    topic, partition, next_offset = position
    cur.execute(
        """
        INSERT INTO consumer_offsets (topic, partition, next_offset) VALUES (%s, %s, %s)
        ON CONFLICT (topic, partition) DO UPDATE
            SET next_offset = GREATEST(consumer_offsets.next_offset, EXCLUDED.next_offset), updated_at = now()
        """,
        (topic, partition, next_offset),
    )


def load_offsets(cur: Cursor) -> dict[tuple[str, int], int]:
    cur.execute("SELECT topic, partition, next_offset FROM consumer_offsets")
    return {(r["topic"], r["partition"]): r["next_offset"] for r in cur.fetchall()}


class EventProcessor:
    def __init__(self, db: Database, settings: Settings, validator: ContractValidator) -> None:
        self.db = db
        self.settings = settings
        self.validator = validator
        self.input_types = set(INPUT_HANDLERS)
        # Posiciones de mensajes ignorados aún sin guardar (se guardan en lote: ver flush_offsets).
        self._deferred: dict[tuple[str, int], int] = {}
        self._last_flush = time.monotonic()

    # --- entrada ------------------------------------------------------------------------
    def handle(self, topic: str, raw: bytes | None, key: str | None, position: Position | None) -> Outcome:
        """Procesa un mensaje tal como llega del broker (o del archivo de eventos)."""
        if raw is None:  # tombstone: nada que aplicar
            self.db.run(lambda cur: save_offset(cur, position))
            return Outcome.IGNORED
        try:
            event = json.loads(raw)
            if not isinstance(event, dict):
                raise ValueError("no es un objeto")
        except ValueError as exc:
            return self._dead_letter(topic, key, raw.decode("utf-8", "replace"), f"mensaje inválido: {exc}",
                                     position)

        if topic == OUTPUT_TOPIC:
            return self._handle_journal(event, position)

        problems = self.validator.problems(event, self.input_types)
        if problems:
            return self._dead_letter(topic, key, event, "; ".join(problems[:5]), position)
        outcome = self._apply_with_retries(topic, event, position)
        if outcome is Outcome.APPLIED:
            self._retry_waiting_for(provides(event))
        return outcome

    def _handle_journal(self, event: dict, position: Position | None) -> Outcome:
        """inventory.events: solo interesan las operaciones de la API, para reconstruir."""
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        if (not self.settings.replay_api_journal or data.get("origin") != API_ORIGIN
                or event.get("event_type") not in JOURNAL_HANDLERS or "event_id" not in event):
            # La mayoría son salidas derivadas: no se abre transacción por cada una. Si el
            # servicio cae antes de guardar, solo se releen mensajes que de todos modos se ignoran.
            if position is not None:
                self._deferred[(position[0], position[1])] = position[2]
                if time.monotonic() - self._last_flush >= 1.0:
                    self.flush_offsets()
            return Outcome.IGNORED
        return self._apply_with_retries(OUTPUT_TOPIC, event, position)

    def flush_offsets(self) -> None:
        """Guarda las posiciones de los mensajes ignorados pendientes."""
        self._last_flush = time.monotonic()
        if not self._deferred:
            return
        pending, self._deferred = self._deferred, {}

        def write(cur: Cursor) -> None:
            for (topic, partition), next_offset in pending.items():
                save_offset(cur, (topic, partition, next_offset))

        self.db.run(write)

    # --- aplicación -----------------------------------------------------------------------
    def _apply_with_retries(self, topic: str, event: dict, position: Position | None,
                            pending_id: int | None = None) -> Outcome:
        attempts = self.settings.max_processing_attempts
        for attempt in range(1, attempts + 1):
            try:
                outcome = self.db.run(lambda cur: self._apply(cur, topic, event, position, pending_id))
                outbox.wake()
                return outcome
            except psycopg.OperationalError:
                raise  # base caída: el consumidor se reinicia desde la última posición guardada
            except InvalidEvent as exc:
                return self._dead_letter(topic, event.get("key"), event, exc.detail, position, pending_id)
            except Exception as exc:
                log.warning("falló %s %s (intento %s/%s): %s", event.get("event_type"), event.get("event_id"),
                            attempt, attempts, exc, exc_info=attempt == attempts)
                if attempt == attempts:
                    return self._dead_letter(topic, event.get("key"), event, f"{type(exc).__name__}: {exc}",
                                             position, pending_id)
                time.sleep(0.2 * attempt)
        raise AssertionError("inalcanzable")

    def _apply(self, cur: Cursor, topic: str, event: dict, position: Position | None,
               pending_id: int | None) -> Outcome:
        event_id, key = event["event_id"], event.get("key") or ""
        cur.execute("SELECT 1 FROM processed_events WHERE event_id = %s", (event_id,))
        if cur.fetchone():
            if pending_id is not None:
                cur.execute("DELETE FROM pending_events WHERE id = %s", (pending_id,))
            save_offset(cur, position)
            return Outcome.DUPLICATE

        if pending_id is None:
            cur.execute("SELECT 1 FROM pending_events WHERE event_id = %s", (event_id,))
            if cur.fetchone():  # reentrega de uno que ya espera
                save_offset(cur, position)
                return Outcome.DUPLICATE
            cur.execute("SELECT 1 FROM pending_events WHERE topic = %s AND event_key = %s LIMIT 1", (topic, key))
            if cur.fetchone():  # otro de la misma llave espera: se forma detrás (orden por llave)
                self._park(cur, topic, event, f"anterior:{key}", None)
                save_offset(cur, position)
                return Outcome.PARKED
        else:
            cur.execute("SELECT 1 FROM pending_events WHERE topic = %s AND event_key = %s AND id < %s LIMIT 1",
                        (topic, key, pending_id))
            if cur.fetchone():
                return Outcome.BLOCKED

        # El efecto y la posición de lectura se confirman juntos: si el servidor cae antes de
        # escribir a disco, al volver se relee desde la posición anterior y se reaplica igual.
        # Por eso aquí no hace falta esperar el fsync.
        cur.execute("SET LOCAL synchronous_commit = off")
        handlers = JOURNAL_HANDLERS if topic == OUTPUT_TOPIC else INPUT_HANDLERS
        uow = UnitOfWork(cur, occurred_at=parse_iso(event["occurred_at"]), source_event_id=event_id)
        cur.execute("SAVEPOINT apply_event")
        try:
            handlers[event["event_type"]](uow, event["data"])
            finish(uow)
        except MissingDependency as dep:
            cur.execute("ROLLBACK TO SAVEPOINT apply_event")
            self._park(cur, topic, event, dep.waiting_for, pending_id)
            save_offset(cur, position)
            return Outcome.PARKED
        except KeyError as exc:
            raise InvalidEvent(f"falta el campo {exc}") from exc
        cur.execute("RELEASE SAVEPOINT apply_event")

        cur.execute("INSERT INTO processed_events (event_id, event_type, topic) VALUES (%s, %s, %s)",
                    (event_id, event["event_type"], topic))
        if pending_id is not None:
            cur.execute("DELETE FROM pending_events WHERE id = %s", (pending_id,))
        save_offset(cur, position)
        return Outcome.APPLIED

    def _park(self, cur: Cursor, topic: str, event: dict, waiting_for: str, pending_id: int | None) -> None:
        if pending_id is None:
            cur.execute(
                """
                INSERT INTO pending_events (event_id, topic, event_type, event_key, payload, waiting_for)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING
                """,
                (event["event_id"], topic, event["event_type"], event.get("key") or "", Jsonb(event), waiting_for),
            )
            log.info("%s %s en espera de %s", event["event_type"], event["event_id"], waiting_for)
        else:
            cur.execute(
                """
                UPDATE pending_events SET attempts = attempts + 1, waiting_for = %s, last_error = %s,
                       next_attempt_at = now() + make_interval(secs => LEAST(60, power(2, attempts)))
                WHERE id = %s
                """,
                (waiting_for, f"falta {waiting_for}", pending_id),
            )

    # --- reintentos -------------------------------------------------------------------------
    def _retry_waiting_for(self, keys: list[str]) -> None:
        """Reintenta solo los eventos que esperaban lo que acaba de llegar."""
        if not keys:
            return
        rows = self.db.run(lambda cur: cur.execute(
            "SELECT id, topic, event_key, payload FROM pending_events WHERE waiting_for = ANY(%s) ORDER BY id",
            (keys,)).fetchall())
        for row in rows:
            self._retry_row(row)

    def _retry_row(self, row: dict) -> Outcome:
        """Reintenta un evento en espera y, si se aplica, a los que esperaban detrás de él
        con la misma llave (en orden de llegada)."""
        outcome = self._apply_with_retries(row["topic"], row["payload"], None, row["id"])
        while outcome in (Outcome.APPLIED, Outcome.DEAD, Outcome.DUPLICATE):
            if outcome is Outcome.APPLIED:
                self._retry_waiting_for(provides(row["payload"]))
            follower = self.db.run(lambda cur: cur.execute(
                "SELECT id, topic, event_key, payload FROM pending_events WHERE topic = %s AND event_key = %s"
                " ORDER BY id LIMIT 1", (row["topic"], row["event_key"])).fetchone())
            if follower is None:
                break
            row = follower
            outcome = self._apply_with_retries(row["topic"], row["payload"], None, row["id"])
        return outcome

    def retry_pending(self, force: bool = False) -> None:
        """Pasada periódica: reintenta el primero de cada llave cuya espera ya venció
        (`force` ignora la espera) y manda a la DLQ lo que esperó demasiado."""
        max_age = self.settings.pending_max_age_seconds
        expired = self.db.run(lambda cur: cur.execute(
            "SELECT id, topic, event_key, payload, waiting_for FROM pending_events"
            " WHERE created_at < now() - make_interval(secs => %s) ORDER BY id", (max_age,)).fetchall())
        for row in expired:
            self._dead_letter(row["topic"], row["event_key"], row["payload"],
                              f"se esperó demasiado a {row['waiting_for']}", None, row["id"])

        heads = self.db.run(lambda cur: cur.execute(
            """
            SELECT p.id, p.topic, p.event_key, p.payload FROM pending_events p
            WHERE (%s OR p.next_attempt_at <= now())
              AND NOT EXISTS (SELECT 1 FROM pending_events q
                              WHERE q.topic = p.topic AND q.event_key = p.event_key AND q.id < p.id)
            ORDER BY p.id
            """, (force,)).fetchall())
        for row in heads:
            self._retry_row(row)

    # --- DLQ ----------------------------------------------------------------------------------
    def _dead_letter(self, topic: str, key: str | None, payload: dict | str, error: str,
                     position: Position | None, pending_id: int | None = None) -> Outcome:
        event = payload if isinstance(payload, dict) else {}
        event_id = event.get("event_id")

        def write(cur: Cursor) -> None:
            if event_id:
                cur.execute(
                    "INSERT INTO processed_events (event_id, event_type, topic) VALUES (%s, %s, %s)"
                    " ON CONFLICT (event_id) DO NOTHING",
                    (event_id, event.get("event_type") or "desconocido", topic),
                )
            if pending_id is not None:
                cur.execute("DELETE FROM pending_events WHERE id = %s", (pending_id,))
            cur.execute(
                "INSERT INTO outbox (event_id, event_type, event_key, payload, topic, headers)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                (str(uuid.uuid4()), event.get("event_type") or "desconocido", key or "", Jsonb(payload), DLQ_TOPIC,
                 Jsonb({"x-error": error[:1000], "x-source-topic": topic})),
            )
            save_offset(cur, position)

        try:
            uuid.UUID(str(event_id)) if event_id else None
        except ValueError:
            event_id = None  # event_id inválido: no se puede registrar en processed_events
        self.db.run(write)
        outbox.wake()
        log.error("a inventory.dlq: %s %s desde %s: %s", event.get("event_type"), event_id, topic, error)
        return Outcome.DEAD
