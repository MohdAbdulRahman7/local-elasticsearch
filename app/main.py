import asyncio
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from . import routes
from . import consumer
from . import rabbitmq

app = FastAPI(title="Local Elasticsearch", description="A simple Elasticsearch-style backend with FastAPI, RabbitMQ, and SQLite")

# CORS for UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routes
app.include_router(routes.router)

# On startup, setup RabbitMQ and start consumers
@app.on_event("startup")
async def startup_event():
    await rabbitmq.setup_rabbitmq()
    asyncio.create_task(consumer.start_consumers())

@app.get("/")
async def root():
    return FileResponse("app/static/index.html")

@app.get("/health")
async def health():
    return {"status": "ok"}