import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from config import settings
from database import Database
from exceptions import OpenPechaError
from observability import setup_telemetry, shutdown_telemetry
from routers.annotation.alignments import router as alignments_router
from routers.annotation.bibliographic import router as bibliographic_router
from routers.annotation.durchens import router as durchens_router
from routers.annotation.paginations import router as paginations_router
from routers.annotation.segmentations import router as segmentations_router
from routers.annotation.table_of_contents import router as table_of_contents_router
from routers.applications import router as applications_router
from routers.categories import router as categories_router
from routers.editions import router as editions_router
from routers.languages import router as languages_router
from routers.persons import router as persons_router
from routers.segments import router as segments_router
from routers.tags import router as tags_router
from routers.texts import router as texts_router
from storage import Storage

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Lifespan context manager for startup/shutdown events."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(name)s - %(message)s")
    logger.info("Starting OpenPecha API")

    setup_telemetry(app)

    if not app.state.testing and settings.neo4j_uri:
        db = Database(
            neo4j_uri=settings.neo4j_uri,
            neo4j_auth=(settings.neo4j_username, settings.neo4j_password),
            neo4j_database=settings.neo4j_database,
        )
        await db.verify_connectivity()
        app.state.db = db
        storage = Storage(bucket_name=settings.aws_s3_bucket, region=settings.aws_region)
        await storage.connect()
        app.state.storage = storage
        logger.info("Database and storage initialized")
        yield
        await storage.close()
        await db.close()
        logger.info("Database and storage connections closed")
    else:
        logger.info("Skipping database initialization (testing mode or no NEO4J_URI)")
        yield

    shutdown_telemetry()
    logger.info("Shutting down OpenPecha API")


def create_app(*, testing: bool = False) -> FastAPI:
    app = FastAPI(
        title="OpenPecha API v2",
        version="2.6.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.state.testing = testing

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

    @app.exception_handler(ValidationError)
    async def validation_exception_handler(_request: Request, exc: ValidationError) -> JSONResponse:
        """Handle Pydantic validation errors."""
        errs = exc.errors()
        first_msg = (errs[0].get("msg") if errs else None) or str(exc) or "Invalid input"
        return JSONResponse(status_code=422, content={"detail": first_msg})

    @app.exception_handler(OpenPechaError)
    async def openpecha_exception_handler(_request: Request, exc: OpenPechaError) -> JSONResponse:
        """Handle OpenPecha custom exceptions."""
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict())

    @app.exception_handler(NotImplementedError)
    async def not_implemented_handler(_request: Request, exc: NotImplementedError) -> JSONResponse:
        """Handle not implemented errors."""
        return JSONResponse(status_code=501, content={"error": str(exc)})

    @app.exception_handler(Exception)
    async def general_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
        """Handle all other exceptions."""
        logger.exception("Exception occurred: %s - %s", type(exc).__name__, str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})

    @app.middleware("http")
    async def request_middleware(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """Log all requests and add no-cache headers to all responses."""
        response = await call_next(request)
        logger.info(
            "Request: %s %s | Status: %d",
            request.method,
            request.url.path,
            response.status_code,
        )
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.get("/__/health")
    async def health_check() -> dict:
        """Health check endpoint."""
        return {"status": "healthy"}

    app.include_router(texts_router)
    app.include_router(editions_router)
    app.include_router(persons_router)
    app.include_router(categories_router)
    app.include_router(tags_router)
    app.include_router(languages_router)
    app.include_router(segmentations_router)
    app.include_router(alignments_router)
    app.include_router(paginations_router)
    app.include_router(table_of_contents_router)
    app.include_router(bibliographic_router)
    app.include_router(durchens_router)
    app.include_router(applications_router)
    app.include_router(segments_router)

    return app


app = create_app()
