from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import air_quality, weather, model
from app.db import engine, Base
from app.routers import openaq as openaq_controller
from app.workers.schedular import start_scheduler

app = FastAPI(title="AirSafe Ghana API")

# CORS for frontend
origins = [
    "http://localhost:5174", 
    "http://127.0.0.1:5174",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,  
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
    # start scheduler - run refresh_all_locations every 60 minutes
    try:
        start_scheduler(interval_minutes=120)
    except Exception as e:
        print("Scheduler failed to start:", e)

# Include routers
app.include_router(openaq_controller.router)
# app.include_router(air_quality.router, prefix="/api")
app.include_router(weather.router, prefix="/api")
app.include_router(model.router, prefix="/api")

@app.get("/")
async def root():
    return {"message": "AirSafe Ghana API"}