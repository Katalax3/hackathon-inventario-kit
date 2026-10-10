"""Validación de los eventos de entrada contra los JSON Schema del contrato."""

from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)


class ContractValidator:
    """Si no encuentra `contract/schemas`, solo revisa que el event_type sea conocido."""

    def __init__(self, schema_dir: str | None) -> None:
        self.validators: dict = {}
        self.input_types: set[str] = set()
        if not schema_dir or not Path(schema_dir).is_dir():
            log.warning("sin carpeta de schemas del contrato: los eventos de entrada no se validan")
            return
        from jsonschema import Draft202012Validator, FormatChecker
        from referencing import Registry, Resource

        root = Path(schema_dir)
        resources = []
        for path in root.rglob("*.schema.json"):
            schema = json.loads(path.read_text(encoding="utf-8"))
            resources.append((schema["$id"], Resource.from_contents(schema)))
        registry = Registry().with_resources(resources)
        index = json.loads((root / "index.json").read_text(encoding="utf-8"))
        for event_type, entry in index["input"].items():
            schema = json.loads((root / entry["schema"]).read_text(encoding="utf-8"))
            self.validators[event_type] = Draft202012Validator(schema, registry=registry,
                                                               format_checker=FormatChecker())
            self.input_types.add(event_type)

    def problems(self, event: dict, known_types: set[str]) -> list[str]:
        event_type = event.get("event_type")
        if event_type not in known_types:
            return [f"event_type desconocido: {event_type!r}"]
        for field in ("event_id", "occurred_at", "data"):
            if field not in event:
                return [f"falta el campo {field}"]
        validator = self.validators.get(event_type)
        if validator is None:
            return []
        return [f"{'/'.join(map(str, e.absolute_path)) or '(raíz)'}: {e.message}"
                for e in validator.iter_errors(event)]
