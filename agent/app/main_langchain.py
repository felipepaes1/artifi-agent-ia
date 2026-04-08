import logging

from fastapi import FastAPI, Request

from .langchain_app.config import load_settings
from .langchain_app.profiles import load_profiles
from .langchain_app.service import WebhookService


logging.basicConfig(level=logging.getLevelName("INFO"))
logger = logging.getLogger("langchain_app")

settings = load_settings()
profiles = load_profiles(settings)
service = WebhookService(settings, profiles)

app = FastAPI()


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "mode": "langchain"}


@app.post("/webhook/waha")
async def waha_webhook(request: Request) -> dict:
    data = await request.json()
    return await service.handle_event(data)
