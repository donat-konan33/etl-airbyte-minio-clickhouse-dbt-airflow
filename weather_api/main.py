
import sys, os
sys.path.append(os.path.dirname(__file__))

from sqlalchemy.orm import Session
from clickhouse_sqlalchemy import make_session
from database import engine
from crud import *
from schemas import *
from typing import List
from fastapi import FastAPI, Depends, HTTPException, Query, Security
from fastapi.security.api_key import APIKeyHeader

import uvicorn

tags = [
    {
        "name": "raw-data",
        "description": "Weather and solar records from the current data mart."
    },
    {
        "name": "metadata",
        "description": "Reference information describing the dates available in the data mart."
    },
    {
        "name": "sunshine",
        "description": "Solar energy and radiation measurements, optionally filtered by region."
    },
    {
        "name": "solar-energy",
        "description": "Estimated solar energy potential and production aggregated by department."
    },
    {
        "name": "solar-geo",
        "description": "Solar measurements with geographic coordinates for a requested date."
    },
    {
        "name": "features",
        "description": "Weather and solar features for a requested department."
    },
    {
        "name": "regional-data",
        "description": "Historical weather and solar measurements aggregated by region and date."
    },
    {
        "name": "departmental-data",
        "description": "Historical weather and solar measurements for a department."
    },
    {
        "name": "ml-data",
        "description": "Archived records intended for machine-learning workflows."
    },
    {
        "name": "temperature",
        "description": "Daily temperature measurements for a requested department."
    },
    {
        "name": "analytics",
        "description": "Summary statistics grouped by geographic level and forecast period."
    }
]

app = FastAPI(
    title="Weather & Solar Energy API",
    description=(
        "Read weather, solar-energy, and geospatial data stored in ClickHouse.\n\n"
        "**Authentication:** All data endpoints require an `X-API-Key` header. "
        "Set the `FAST_API_KEY` environment variable before starting the API, "
        "then use the **Authorize** button in this documentation to provide the key. "
        "A missing key returns "
        "`403`; an invalid key returns `401`. The `/` and `/health` endpoints are public.\n\n"
        "**Empty results:** Data endpoints return `404` when no matching records "
        "are available. `/health` returns `503` when the database cannot be reached."
    ),
    version="1.0.0",
    openapi_tags=tags,
)

api_key_header = APIKeyHeader(
    name="X-API-Key",
    description="API key configured through the FAST_API_KEY environment variable.",
)

API_KEY = os.environ.get("FAST_API_KEY")

def verify_api_key(api_key: str = Security(api_key_header)):
    if not API_KEY:
        raise RuntimeError("API is not configured with an API key. Please set the FAST_API_KEY environment variable.")
    if api_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    return api_key

dependencies=[Depends(verify_api_key)]

def get_db():
   """Yield a database session and close it after the request completes."""
   db = make_session(engine)
   try:
       yield db
   finally:
       db.close()


@app.get(
    "/",
    summary="Check that the API is running",
    description="Returns the API status and links to the interactive documentation.",
)
def _read_root():
   return {
       "Weather App": "Running",
       "API": "Weather & Solar Energy API",
       "docs": "/docs",
       "redoc": "/redoc"
   }


@app.get(
    "/health",
    summary="Check API and database health",
    description="Runs a lightweight database query to verify ClickHouse connectivity.",
    responses={503: {"description": "The database is unavailable."}},
)
def _health_check():
    db = make_session(engine)
    try:
        db.execute("SELECT 1")
        return {
            "status": "ok",
            "database": "ok"
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Database unavailable {str(e)}"
        )
    finally:
        db.close()

@app.get(
    "/get_data",
    tags=["raw-data"],
    dependencies=dependencies,
    summary="Retrieve weather and solar records",
    description="Returns all available records from the current weather and solar data mart.",
    responses={404: {"description": "No data is available."}},
)
def _get_data(db: Session = Depends(get_db)):
   data = get_data(db)
   if not data:
       raise HTTPException(status_code=404, detail="All Data not found")
   return data

@app.get(
    "/date",
    tags=["metadata"],
    dependencies=dependencies,
    summary="List dates available in the data mart",
    description="Returns the distinct dates for which weather and solar records are available.",
    responses={404: {"description": "No dates are available."}},
)
def _get_date(db: Session = Depends(get_db)):
   dates = get_date(db)
   if not dates:
       raise HTTPException(status_code=404, detail="Dates not found")
   return dates

@app.get(
    "/get_sunshine_data",
    tags=["sunshine"],
    dependencies=dependencies,
    summary="Retrieve sunshine measurements",
    description=(
        "Returns solar energy and solar radiation measurements with their date, "
        "region, and department."
    ),
    responses={404: {"description": "No sunshine measurements are available."}},
)
def _get_sunshine_data(db: Session = Depends(get_db)):
    data = get_sunshine_data(db)
    if not data:
        raise HTTPException(status_code=404, detail="Sunshine data not found")
    return data

@app.get(
    "/solar_geo_data",
    tags=["solar-geo"],
    dependencies=dependencies,
    summary="Retrieve geospatial solar data for a date",
    description=(
        "Returns solar energy and radiation measurements, geographic coordinates, "
        "GeoJSON, and regional averages for the requested date."
    ),
    responses={404: {"description": "No geospatial solar data exists for this date."}},
)
def _get_solarenergy_geo_data(
    date: str = Query(
        ...,
        description="Date to retrieve, in the format used by the dataset (for example, `2024-01-15`).",
    ),
    db: Session = Depends(get_db),
):
    data = get_solarenergy_geo_data_data(db=db, date=date)
    if not data:
        raise HTTPException(status_code=404, detail="Solar Geo Data not found")
    return data

@app.get(
    "/common_features",
    tags=["features"],
    dependencies=dependencies,
    summary="Retrieve common weather and solar features",
    description=(
        "Returns dated measurements for a department, including wind speed and gust, "
        "pressure, solar energy, temperature, feels-like temperature, and precipitation."
    ),
    responses={404: {"description": "No feature records exist for this department."}},
)
def _get_tfptwgp(
    department: str = Query(
        ...,
        description="Department code or name, matching the value stored in the dataset.",
    ),
    db: Session = Depends(get_db),
):
    data = get_tfptwgp(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Common features Data not found")
    return data

@app.get(
    "/get_region_sunshine_data",
    tags=["sunshine"],
    dependencies=dependencies,
    summary="Retrieve sunshine measurements for a region",
    description=(
        "Returns solar energy and radiation measurements for departments belonging "
        "to the requested region name."
    ),
    responses={404: {"description": "No sunshine measurements exist for this region."}},
)
def _get_region_sunshine_data(
    region: str = Query(
        ...,
        description="Region name, matching the value stored in the dataset.",
    ),
    db: Session = Depends(get_db),
):
    data = get_region_sunshine_data(db=db, region=region)
    if not data:
        raise HTTPException(status_code=404, detail="Region Sunshine data Data not found")
    return data

@app.get(
    "/get_solarenergy_agg_pday",
    tags=["solar-energy"],
    dependencies=dependencies,
    summary="Estimate daily solar production for a department",
    description=(
        "Returns the department's average solar energy per square metre, the estimated "
        "energy incident on a 2.7 m² panel, and estimated electrical production using "
        "the Trina Solar TSM-DE19R's maximum 21.7% efficiency. The datasheet specifies "
        "a module power range of 565–585 W (up to 585 W). This is a theoretical estimate "
        "and does not account for installation conditions or system losses.\n\n"
        "Reference: [Trina Solar TSM-DE19R datasheet]"
        "(https://static.trinasolar.com/sites/default/files/"
        "Datasheet_Vertex_DE19R_FR_2023%20C_web.pdf)."
    ),
    responses={404: {"description": "No solar-energy records exist for this department."}},
)
def _get_solarenergy_agg_pday(
    department: str = Query(
        ...,
        description="Department code or name, matching the value stored in the dataset.",
    ),
    db: Session = Depends(get_db),
):
    data = get_solarenergy_agg_pday(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Daily Solar Aggregating data not found")
    return data

@app.get(
    "/get_entire_region_data",
    tags=["regional-data"],
    dependencies=dependencies,
    summary="Retrieve historical data for a region",
    description=(
        "Returns historical daily regional averages for solar energy, solar radiation, "
        "temperature, precipitation, and UV index."
    ),
    responses={404: {"description": "No historical records exist for this region."}},
)
def _get_entire_region_data(
    region: str = Query(
        ...,
        description="Region name, matching the value stored in the dataset.",
    ),
    db: Session = Depends(get_db),
):
    data = get_entire_region_data(db=db, region=region)
    if not data:
        raise HTTPException(status_code=404, detail="Entire Region data not found")
    return data

@app.get(
    "/get_entire_department_data",
    tags=["departmental-data"],
    dependencies=dependencies,
    summary="Retrieve historical data for a department",
    description=(
        "Returns historical daily solar energy, solar radiation, temperature, "
        "precipitation, and UV index measurements for the requested department."
    ),
    responses={404: {"description": "No historical records exist for this department."}},
)
def _get_entire_department_data(
    department: str = Query(
        ...,
        description="Department code or name, matching the value stored in the dataset.",
    ),
    db: Session = Depends(get_db),
):
    data = get_entire_department_data(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Entire Department data not found")
    return data

@app.get(
    "/get_ml_data",
    tags=["ml-data"],
    dependencies=dependencies,
    summary="Retrieve archived data for machine learning",
    description="Returns all available archived weather and solar records.",
    responses={404: {"description": "No archived records are available."}},
)
def _get_entire_data(db: Session = Depends(get_db)):
    data = get_ml_data(db=db)
    if not data:
        raise HTTPException(status_code=404, detail="Entire data so far not found")
    return data

@app.get(
    "/get_temp_data",
    tags=["temperature"],
    dependencies=dependencies,
    summary="Retrieve temperature data for a department",
    description=(
        "Returns daily actual, minimum, maximum, and feels-like temperatures, "
        "along with the date, weekday, and weather description."
    ),
    responses={404: {"description": "No temperature records exist for this department."}},
)
def _get_temp_data(
    department: str = Query(
        ...,
        description="Department code or name, matching the value stored in the dataset.",
    ),
    db: Session = Depends(get_db),
):
    data = get_temp_data(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Entire Department data not found")
    return data


@app.get(
    "/analytics/stats",
    tags=["analytics"],
    dependencies=dependencies,
    summary="Retrieve weather and solar summary statistics",
    description=(
        "Returns average values for the available weather and solar metrics, grouped "
        "by region or department. The selected period is today or the next three days. "
        "The `top` parameter is currently accepted for compatibility but does not "
        "change the returned rows."
    ),
    responses={
        400: {"description": "The requested level or period is not supported."},
        404: {"description": "No statistics are available for the requested filters."},
    },
)
def _get_stats(
        level: str = Query(
            "department",
            description="Geographic grouping: `region` or `department`.",
        ),
        period: str = Query(
            "today",
            description="Data period: `today` or `next3days`.",
        ),
        top: bool = Query(
            True,
            description=(
                "Compatibility flag intended to select the top or bottom three "
                "results. It currently does not affect the returned rows."
            ),
        ),
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
