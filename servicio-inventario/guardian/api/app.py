"""Aplicación FastAPI: errores con la forma del contrato y arranque de los hilos de fondo."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .. import __version__
from ..errors import GuardianError
from ..service import Guardian
from .routes import router


def create_app(guardian: Guardian) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        guardian.start()
        try:
            yield
        finally:
            guardian.shutdown()

    app = FastAPI(title="Guardian — servicio de inventario del taller", version=__version__, lifespan=lifespan)
    app.state.guardian = guardian
    app.include_router(router)

    # Errores: siempre {"detail": "<mensaje>"} (contract/openapi.yaml).
    @app.exception_handler(GuardianError)
    async def domain_error(_: Request, exc: GuardianError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        messages = []
        for error in exc.errors():
            where = ".".join(str(p) for p in error.get("loc", ())[1:]) or "cuerpo"
            messages.append(f"{where}: {error.get('msg')}")
        return JSONResponse(status_code=422, content={"detail": "; ".join(messages) or "petición inválida"})

    return app
