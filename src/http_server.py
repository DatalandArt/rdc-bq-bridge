"""Health and Prometheus metrics HTTP server.

Runs alongside the asyncio pipeline as a non-critical task. Exposes:
  - GET /healthz : liveness (always 200 while the event loop is responsive)
  - GET /readyz  : readiness (200 only when all components are up)
  - GET /metrics : Prometheus exposition format
"""

import logging
from typing import TYPE_CHECKING

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

if TYPE_CHECKING:
    from .bq_loader import BigQueryLoaderManager
    from .device_ticket_mapper import DeviceTicketMapper
    from .redis_ingestor import RedisIngestor
    from .row_assembler import RowAssembler

logger = logging.getLogger(__name__)


class HttpServer:
    """Serves /healthz, /readyz, and /metrics on a single port."""

    def __init__(
        self,
        host: str,
        port: int,
        redis_ingestor: "RedisIngestor",
        row_assembler: "RowAssembler",
        bq_loader_manager: "BigQueryLoaderManager",
        device_mapper: "DeviceTicketMapper",
    ):
        self.host = host
        self.port = port
        self.redis_ingestor = redis_ingestor
        self.row_assembler = row_assembler
        self.bq_loader_manager = bq_loader_manager
        self.device_mapper = device_mapper
        self.app = self._build_app()

    def _build_app(self) -> FastAPI:
        app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

        @app.get("/healthz")
        async def healthz() -> JSONResponse:
            return JSONResponse({"status": "ok"})

        @app.get("/readyz")
        async def readyz() -> JSONResponse:
            checks = {
                "redis_connected": self.redis_ingestor.connection_manager.redis_client is not None,
                "row_assembler_running": self.row_assembler._running,
                "loaders_running": all(
                    loader._running for loader in self.bq_loader_manager.loaders.values()
                ) if self.bq_loader_manager.loaders else False,
            }
            ready = all(checks.values())
            return JSONResponse(
                {"ready": ready, "checks": checks},
                status_code=200 if ready else 503,
            )

        @app.get("/metrics")
        async def metrics_endpoint() -> Response:
            return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

        return app

    async def run(self) -> None:
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_config=None,
            access_log=False,
            lifespan="off",
        )
        server = uvicorn.Server(config)
        logger.info(f"Metrics server listening on http://{self.host}:{self.port}")
        await server.serve()
