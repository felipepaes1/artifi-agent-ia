import logging
import os

from fastapi import FastAPI

from .chatwoot_integration import build_chatwoot_router
from .config.settings import OPENAI_API_KEY, SUPABASE_ENABLED, SUPABASE_KEY, SUPABASE_URL
from .core.profiles import log_profile_knowledge_status
from .handlers.chat import build_chat_router
from .handlers.waha_webhook import build_waha_router, send_text_parts
from .integrations.supabase import supabase_create_client


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("agent")

if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY (or API_OPENAI_KEY) is not set. The agent will fail on first request.")

if SUPABASE_ENABLED and (not SUPABASE_URL or not SUPABASE_KEY):
    logger.warning("Supabase is enabled but SUPABASE_URL / SUPABASE_KEY are missing.")
if SUPABASE_ENABLED and supabase_create_client is None:
    logger.warning("Supabase client is not installed. Run `pip install supabase` in the agent environment.")

log_profile_knowledge_status()

app = FastAPI()
app.include_router(build_chat_router())
app.include_router(build_waha_router())
app.include_router(build_chatwoot_router(send_text_parts))
