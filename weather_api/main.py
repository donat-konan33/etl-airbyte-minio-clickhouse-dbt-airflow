
import sys, os
sys.path.append(os.path.dirname(__file__))

from sqlalchemy.orm import Session
from clickhouse_sqlalchemy import make_session
from database import engine
from crud import *
from schemas import *
from typing import List
from fastapi import FastAPI, Depends, HTTPException, Query
import uvicorn

tags = [
    {
        "name": "users",
        "description": "Operations with users"
    },
    {
        "name": "raw-data",
        "description": "Access raw, unaggregated weather and solar datasets."
    },
    {
        "name": "metadata",
        "description": "Retrieve reference information like available dates or valid regions."
    },
    {
        "name": "sunshine",
        "description": "Endpoints related to sunshine duration and exposure metrics."
    },
    {
        "name": "solar-energy",
        "description": "Aggregated solar energy production or potential data."
    },
    {
        "name": "solar-geo",
        "description": "Geospatial solar data linked to specific dates."
    },
    {
        "name": "features",
        "description": "Engineered or common features used for analytics or modeling."
    },
    {
        "name": "regional-data",
        "description": "Weather and solar data aggregated at the region level."
    },
    {
        "name": "departmental-data",
        "description": "Weather and solar data aggregated at the department level."
    },
    {
        "name": "ml-data",
        "description": "Curated datasets ready for machine learning pipelines."
    },
    {
        "name": "temperature",
        "description": "Temperature-related metrics by geographic area."
    }
]

app = FastAPI(title="Weather Database", openapi_tags=tags)

def get_db():
   """Helper function which opens a connection to the database and also manages closing the connection"""
   db = make_session(engine)
   try:
       yield db
   finally:
       db.close()


# App landing page
@app.get("/")
def _read_root():
   return {
       "Weather App": "Running",
       "API": "Weather & Solar Energy API",
       "docs": "/docs",
       "redoc": "/redoc"
   }

@app.get("/get_data", tags="raw-data")
def _get_data(db: Session = Depends(get_db)):
   data = get_data(db)
   if not data:
       raise HTTPException(status_code=404, detail="All Data not found")
   return data

@app.get("/date", tags="metadata")
def _get_date(db: Session = Depends(get_db)):
   dates = get_date(db)
   if not dates:
       raise HTTPException(status_code=404, detail="Dates not found")
   return dates

@app.get("/get_sunshine_data", tags="sunshine")
def _get_sunshine_data(db: Session = Depends(get_db)):
    data = get_sunshine_data(db)
    if not data:
        raise HTTPException(status_code=404, detail="Sunshine data not found")
    return data

@app.get("/solar_geo_data", tags="solar-geo")
def _get_solarenergy_geo_data(date: str, db: Session = Depends(get_db)):
    data = get_solarenergy_geo_data_data(db=db, date=date)
    if not data:
        raise HTTPException(status_code=404, detail="Solar Geo Data not found")
    return data

@app.get("/common_features", tags="features")
def _get_tfptwgp(department: str, db: Session = Depends(get_db)):
    data = get_tfptwgp(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Common features Data not found")
    return data

@app.get("/get_region_sunshine_data", tags="sunshine")
def _get_region_sunshine_data(region: str, db: Session = Depends(get_db)):
    data = get_region_sunshine_data(db=db, region=region)
    if not data:
        raise HTTPException(status_code=404, detail="Region Sunshine data Data not found")
    return data

@app.get("/get_solarenergy_agg_pday", tags="solar-energy")
def _get_solarenergy_agg_pday(department: str, db: Session = Depends(get_db)):
    data = get_solarenergy_agg_pday(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Daily Solar Aggregating data not found")
    return data

@app.get("/get_entire_region_data", tags="regional-data")
def _get_entire_region_data(region: str, db: Session = Depends(get_db)):
    data = get_entire_region_data(db=db, region=region)
    if not data:
        raise HTTPException(status_code=404, detail="Entire Region data not found")
    return data

@app.get("/get_entire_department_data", tags="departmental-data")
def _get_entire_department_data(department: str, db: Session = Depends(get_db)):
    data = get_entire_department_data(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Entire Department data not found")
    return data

@app.get("/get_ml_data", tags="ml-data")
def _get_entire_data(db: Session = Depends(get_db)):
    data = get_ml_data(db=db)
    if not data:
        raise HTTPException(status_code=404, detail="Entire data so far not found")
    return data

@app.get("/get_temp_data", tags="temperature")
def _get_temp_data(department: str, db: Session = Depends(get_db)):
    data = get_temp_data(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Entire Department data not found")
    return data


@app.get("/analytics/stats")
def _get_stats(
        level: str = Query("department", description="region or department"),
        period: str = Query("today", description="today or next3days"),
        top: bool = Query(True, description="True for top3, False for bottom3"),
        db: Session = Depends(get_db),
):

    if level not in ("region", "department"):
        raise HTTPException(status_code=400, detail="level must be 'region' or 'department'")
    if period not in ("today", "next3days"):
        raise HTTPException(status_code=400, detail="period must be 'today' or 'next3days'")

    data = get_stats(level=level, period=period, top=top, db=db)
    if not data:
        raise HTTPException(status_code=404, detail="Stats data not found")
    return data


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8005)
