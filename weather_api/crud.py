import sys, os
sys.path.append(os.path.dirname(__file__))

from sqlalchemy.orm import Session
import pandas as pd
from sqlalchemy import text, func, desc, asc
from models import mart_newdata_, archived_data, mart_today_stats, mart_next_3_days_stats, mart_today, mart_next_3_days
from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection # for later use with FastAPI and async calls


def get_data(db: Session):
    """Get daily extracted data from the table"""
    return db.query(mart_newdata_).all()


def get_temp_data(db: Session, department: str):
    """Get temperature data like temp, fileslikemin, fileslikemax and feelslike
    """
    return db.query(mart_newdata_) \
            .with_entities(
                mart_newdata_.c.dates,
                mart_newdata_.c.weekday_name,
                mart_newdata_.c.department,
                mart_newdata_.c.temp,
                mart_newdata_.c.tempmin,
                mart_newdata_.c.tempmax,
                mart_newdata_.c.feelslike,
                mart_newdata_.c.feelslikemin,
                mart_newdata_.c.feelslikemax,
                mart_newdata_.c.descriptions
            ).where(mart_newdata_.c.department == department) \
            .order_by(mart_newdata_.c.dates).all()


def get_solarenergy_geo_data_data(db: Session, date: str): # faudra voir la Session(importé de sqlachelmy.orm) venant de sqlalchemy Core
    """Get solarenergy_kwhpm2, solarradiation, reg_name, avg_solarenergy_kwhpm2,
    avg_solarradiation"""
    return db.query(mart_newdata_) \
        .with_entities(
            mart_newdata_.c.dates,
            mart_newdata_.c.weekday_name,
            mart_newdata_.c.department,
            mart_newdata_.c.geo_point_2d,
            mart_newdata_.c.geojson,
            mart_newdata_.c.solarenergy_kwhpm2,
            mart_newdata_.c.solarradiation,
            mart_newdata_.c.reg_name,
            mart_newdata_.c.avg_solarenergy_kwhpm2,
            mart_newdata_.c.avg_solarradiation
        ).where(mart_newdata_.c.dates == date).all()


def get_date(db: Session):
    """
	Get Week current date from data recorded in the table
    """
    rows = db.query(mart_newdata_.c.dates).distinct().all()
    return [row[0] for row in rows]


def get_tfptwgp(db: Session, department: str):
    """
    Get some interesting features like tfptwgp as :
    Temperature, Feels like, Pecipitation, Wind, Gust and Pressure
    """
    return db.query(mart_newdata_) \
        .with_entities(
            mart_newdata_.c.dates,
            mart_newdata_.c.department,
            mart_newdata_.c.reg_name,
            mart_newdata_.c.windspeed,
            mart_newdata_.c.windgust,
            mart_newdata_.c.pressure,
            mart_newdata_.c.solarenergy_kwhpm2,
            mart_newdata_.c.temp,
            mart_newdata_.c.feelslike,
            mart_newdata_.c.precip
        ).where(mart_newdata_.c.department == department) \
        .order_by(mart_newdata_.c.dates).all()


def get_sunshine_data(db: Session):
    """
    Get some interesting features like tfptwgp as :
    Temperature, Feels like, Pecipitation, Wind, Gust and Pressure
    """
    return db.query(mart_newdata_.c.dates,
                    mart_newdata_.c.reg_name,
                    mart_newdata_.c.department,
                    mart_newdata_.c.solarenergy_kwhpm2,
                    mart_newdata_.c.solarradiation
                    ).all()


def get_region_sunshine_data(db: Session, region:str):
    """
    Get sunshine data for a specific region
    """
    return db.query(mart_newdata_) \
        .with_entities(
            mart_newdata_.c.reg_name,
            mart_newdata_.c.department,
            mart_newdata_.c.solarenergy_kwhpm2,
            mart_newdata_.c.solarradiation
        ).filter(mart_newdata_.c.reg_name == region).all()


def get_solarenergy_agg_pday(db: Session, department:str):
    """
       We take into account the calculation over 8 days as recorded
       Panel area = 2.7 m²
       Panel efficiency = 21.7%
    """
    result = (
    db.query(
        mart_newdata_.c.department,
        func.avg(mart_newdata_.c.solarenergy_kwhpm2) \
            .label("solarenergy_kwhpm2"),
        (func.avg(mart_newdata_.c.solarenergy_kwhpm2) * 2.7) \
            .label("available_solarenergy_kwhc"),
        (func.avg(mart_newdata_.c.solarenergy_kwhpm2) * 2.7 * 0.217) \
            .label("real_production_kwhpday"),
    )
    .where(mart_newdata_.c.department == department)
    .group_by(mart_newdata_.c.department)
    .all()
)
    return result


# we can use execute with text query for more complex queries
# checck test from database.py
def get_entire_department_data(db: Session, department:str):
    """
       Get local entire data for a department from data agregated so far
    """
    return db.query(
                      archived_data.c.dates,
                      archived_data.c.department,
                      func.round(archived_data.c.solarenergy_kwhpm2, 1) \
                          .label("solarenergy_kwhpm2"),
                      func.round(archived_data.c.solarradiation, 1) \
                          .label("solarradiation"),
                      func.round(archived_data.c.temp, 1) \
                          .label("temp"),
                      func.round(archived_data.c.precip, 1) \
                          .label("precip"),
                      func.round(archived_data.c.uvindex, 1) \
                          .label("uvindex")
                      ).filter(archived_data.c.department == department) \
                          .order_by(archived_data.c.dates).all()


def get_entire_region_data(db: Session, region:str):
    """
    Get local entire data for a region from data agregated so far
    """
    return db.query(
        archived_data.c.dates,
        archived_data.c.reg_name,
        func.round(func.avg(archived_data.c.solarenergy_kwhpm2), 1) \
                .label("solarenergy_kwhpm2"),
            func.round(func.avg(archived_data.c.solarradiation), 1) \
                .label("solarradiation"),
            func.round(func.avg(archived_data.c.temp), 1) \
                .label("temp"),
            func.round(func.avg(archived_data.c.precip), 1) \
                .label("precip"),
            func.round(func.avg(archived_data.c.uvindex), 1) \
                .label("uvindex")
                      ) \
            .where(archived_data.c.reg_name == region) \
            .group_by(archived_data.c.dates, archived_data.c.reg_name) \
            .order_by(archived_data.c.dates, archived_data.c.reg_name).all()


def get_ml_data(db: Session):
    """get all available data that can be useful for training forecasting model"""
    return db.query(archived_data).all()


def get_today_stats(db: Session):
    """Get today's weather stats for all departments"""
    return db.query(mart_today_stats).all()


def get_next_3_days_stats(db: Session, department: str):
    """Get next 3 days weather forecast for a specific department"""
    return db.query(mart_next_3_days_stats).all()


def get_next_3_days_data(db: Session):
    """Get next 3 days weather data for all departments"""
    return db.query(mart_next_3_days).all()


def get_today_data(db: Session):
    """Get today's weather data for all departments"""
    return db.query(mart_today).all()

def get_stats(
    db: Session,
    level: str = "department",
    period: str = "today",
    top: bool = True

):

    # choisir le modèle ORM selon period
    Model = mart_today if period == "today" else mart_next_3_days

    # champ de group by selon level
    group_field = Model.c.reg_name if level == "region" else Model.c.department

    # requête ORM
    query = (
        db.query(
            group_field.label("geo_name"),
            func.round(func.avg(Model.c.temp), 1).label("temperature"), #°C
            func.round(func.avg(Model.c.humidity), 1).label("humidity"), #%
            func.round(func.avg(Model.c.windspeed), 1).label("windspeed"), #kph
            func.round(func.avg(Model.c.pressure), 1).label("pressure"), #mb
            func.round(func.avg(Model.c.cloudcover), 1).label("cloudcover"), #%
            func.round(func.avg(Model.c.solarradiation), 1).label("solarradiation"), #W/m²
            func.round(func.avg(Model.c.solarenergy_kwhpm2), 2).label("solarenergy"), #kWh/m²
        )
        .group_by(group_field)
    )

    result = query.all()  # ORM → récupère tous les résultats
    return [dict(row._mapping) for row in result]  # row._mapping permet de transformer en dict
