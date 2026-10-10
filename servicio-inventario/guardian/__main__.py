"""Línea de comandos.

    python -m guardian serve                 API + consumidor + publicador (por defecto)
    python -m guardian migrate               crea o actualiza el esquema
    python -m guardian reset-db --yes        borra la base para reconstruir desde los tópicos
    python -m guardian load-file EVENTS      aplica un archivo .jsonl sin broker (desarrollo)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path

from .config import CATALOG_TOPIC, Settings


def _serve(settings: Settings) -> None:
    import uvicorn

    from .api.app import create_app
    from .service import Guardian

    uvicorn.run(create_app(Guardian(settings)), host=settings.api_host, port=settings.api_port,
                log_level=settings.log_level.lower())


def _load_file(settings: Settings, path: Path) -> None:
    """Aplica el archivo como lo entregaría el broker: primero todo el catálogo (regla 22),
    luego lo demás en el orden del archivo. Lo que se emita queda en `outbox` y se publica
    cuando arranque `serve`."""
    from .service import Guardian

    guardian = Guardian(settings)
    guardian.open()
    records = [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]
    ordered = [r for r in records if r["topic"] == CATALOG_TOPIC] + [r for r in records if r["topic"] != CATALOG_TOPIC]
    outcomes: Counter[str] = Counter()
    for record in ordered:
        event = record["event"]
        outcome = guardian.processor.handle(record["topic"], json.dumps(event).encode(), event.get("key"), None)
        outcomes[outcome.value] += 1
    guardian.processor.retry_pending(force=True)
    guardian.db.close()
    print(f"{len(records)} entregas: " + ", ".join(f"{n} {k}" for k, n in outcomes.most_common()))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="guardian", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("serve", help="API + consumidor + publicador")
    sub.add_parser("migrate", help="crea o actualiza el esquema")
    reset = sub.add_parser("reset-db", help="borra todo y recrea el esquema")
    reset.add_argument("--yes", action="store_true", help="confirma el borrado")
    load = sub.add_parser("load-file", help="aplica un archivo de eventos .jsonl sin broker")
    load.add_argument("path", type=Path)
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    logging.basicConfig(level=settings.log_level.upper(),
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    command = args.command or "serve"
    if command == "serve":
        _serve(settings)
    elif command == "migrate":
        from .db import Database
        db = Database(settings.database_url, 2)
        db.open()
        applied = db.migrate()
        print("esquema al día" + (f"; aplicadas: {', '.join(applied)}" if applied else ""))
        db.close()
    elif command == "reset-db":
        if not args.yes:
            sys.exit("reset-db borra todo el inventario. Repite con --yes para confirmar.")
        from .db import Database
        db = Database(settings.database_url, 2)
        db.open()
        try:
            db.reset()
        except RuntimeError as exc:
            sys.exit(f"reset-db cancelado: {exc}")
        finally:
            db.close()
        print("base borrada y esquema recreado: al arrancar se releerán los tópicos desde el inicio")
    elif command == "load-file":
        _load_file(settings, args.path)


if __name__ == "__main__":
    main()
