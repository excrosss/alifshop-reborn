from fastapi import FastAPI
from app.api.routes import router

app = FastAPI(title="Alif Admin API")
app.include_router(router)
