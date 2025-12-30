# app/main.py
from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .config import settings
from .database import Base, engine
from .routes_health import router as health_router
from .routes_tips import router as tips_router
from .routes_stats import router as stats_router
from .routes_admin import router as admin_router
from .routes_cron import router as cron_router
from .routes_ui import router as ui_router
from .routes_ui_overview import router as ui_overview_router
from .routes_debug import router as debug_router
from .routes_trends import router as trends_router
from .routes_reasoning import router as reasoning_router

Base.metadata.create_all(bind=engine)

app = FastAPI(title=settings.app_name)

app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(health_router)
app.include_router(tips_router)
app.include_router(stats_router)
app.include_router(admin_router)
app.include_router(cron_router)
app.include_router(ui_router)
app.include_router(ui_overview_router)
app.include_router(debug_router)
app.include_router(trends_router)
app.include_router(reasoning_router)
