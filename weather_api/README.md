# Weather & Solar Energy API

A FastAPI-based REST API for accessing weather and solar energy data stored in ClickHouse database. This API provides comprehensive endpoints for retrieving weather forecasts, solar energy metrics, and analytical insights across different geographical levels (regions and departments).

## Features

- **Weather Data**: Access temperature, precipitation, wind speed, and weather descriptions
- **Solar Energy**: Solar radiation and energy production metrics (kWh/m²)
- **Geospatial Data**: Location-based data with geo coordinates and GeoJSON
- **Analytics**: Statistical insights and aggregations by region/department
- **ML-Ready Data**: Curated datasets prepared for machine learning pipelines
- **User Management**: Basic user authentication system

## Technology Stack

- **Framework**: FastAPI
- **Database**: ClickHouse (via clickhouse-sqlalchemy)
- **ORM**: SQLAlchemy
- **Validation**: Pydantic
- **ASGI Server**: Uvicorn

## Database Tables

The API interacts with several ClickHouse tables:
- `mart_newdata_`: Main weather and solar data table
- `archived_data`: Historical weather data
- `mart_today` & `mart_next_3_days`: Current and forecast data marts
- `mart_today_stats` & `mart_next_3_days_stats`: Statistical aggregations
- `users`: User authentication data
- `raw_depcode`: Department/location reference data

## API Endpoints

### Core Endpoints

- `GET /` - API status and documentation links
- `GET /docs` - Interactive API documentation (Swagger UI)
- `GET /redoc` - Alternative API documentation

### Data Endpoints

#### Raw Data
- `GET /get_data` - Retrieve all raw weather and solar data

#### Metadata
- `GET /date` - Get available dates in the dataset

#### Sunshine & Solar Energy
- `GET /get_sunshine_data` - Sunshine duration and exposure metrics
- `GET /solar_geo_data?date={date}` - Geospatial solar data for specific date
- `GET /get_solarenergy_agg_pday?department={dept}` - Daily solar energy aggregations

#### Regional & Departmental Data
- `GET /get_region_sunshine_data?region={region}` - Sunshine data by region
- `GET /get_entire_region_data?region={region}` - Complete regional weather data
- `GET /get_entire_department_data?department={dept}` - Complete departmental weather data

#### Temperature Data
- `GET /get_temp_data?department={dept}` - Temperature metrics (actual, min, max, feels-like)

#### Features & ML Data
- `GET /common_features?department={dept}` - Engineered features for analytics
- `GET /get_ml_data` - Curated datasets for machine learning

#### Analytics
- `GET /analytics/stats?level={region|department}&period={today|next3days}&top={true|false}` - Statistical insights

## Data Models

### Weather Data Fields
- `dates`: Date of the weather data
- `weekday_name`: Day of the week
- `department`: French department code/name
- `reg_name`: Region name
- `temp`, `tempmin`, `tempmax`: Temperature metrics (°C)
- `feelslike`, `feelslikemin`, `feelslikemax`: Feels-like temperature
- `descriptions`: Weather description
- `windspeed`, `windgust`: Wind metrics
- `pressure`: Atmospheric pressure
- `precip`: Precipitation
- `solarenergy_kwhpm2`: Solar energy production (kWh/m²)
- `solarradiation`: Solar radiation
- `geo_point_2d`: Geographic coordinates
- `geojson`: GeoJSON geometry data

## Setup & Installation

1. **Environment Variables**
   ```bash
   export CLICKHOUSE_DATABASE_URL="clickhouse://user:password@host:port/database"
   ```

2. **Install Dependencies**
   This project uses Poetry for dependency management. From the project root directory:
   ```bash
   poetry install --with api
   ```

3. **Run the API**
   ```bash
   poetry run python weather_api/main.py
   ```

   Or with uvicorn:
   ```bash
   poetry run uvicorn weather_api.main:app --host 0.0.0.0 --port 8005 --reload
   ```

## Usage Examples

### Get Today's Weather for a Department
```bash
curl "http://localhost:8005/get_temp_data?department=75"
```

### Get Solar Energy Data for Specific Date
```bash
curl "http://localhost:8005/solar_geo_data?date=2024-01-15"
```

### Get Analytics Stats
```bash
curl "http://localhost:8005/analytics/stats?level=department&period=today&top=true"
```

## API Documentation

Once running, visit:
- **Swagger UI**: http://localhost:8005/docs
- **ReDoc**: http://localhost:8005/redoc

## Project Structure

```
weather_api/
├── main.py          # FastAPI application and endpoints
├── models.py        # SQLAlchemy models and table definitions
├── crud.py          # Database operations and queries
├── schemas.py       # Pydantic schemas for request/response validation
├── database.py      # Database connection configuration
├── __init__.py      # Package initialization
└── README.md        # This file
```

## Contributing

This API is part of a larger ETL pipeline that includes Airbyte, MinIO, ClickHouse, DBT, and Airflow. Ensure database tables are properly populated before using the API endpoints.
