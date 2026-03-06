import sys, os
sys.path.append(os.path.dirname(__file__))

from database import engine
from sqlalchemy import Column, MetaData, literal
from clickhouse_sqlalchemy import (
    Table, make_session, engines, types, get_declarative_base
)
session = make_session(engine)
metadata = MetaData(bind=engine)
Base = get_declarative_base(metadata=metadata)

archived_data = Table('archived_data', metadata, autoload=True) # we must reflect table from database

mart_today = Table('mart_today', metadata, autoload=True)
mart_next_3_days = Table('mart_next_3_days', metadata, autoload=True)
mart_today_stats = Table('mart_today_stats', metadata, autoload=True)
mart_next_3_days_stats = Table('mart_next_3_days_stats', metadata, autoload=True)


class Users(Base):
    """class representing the user table"""
    __tablename__ = "users"

    id = Column(types.Int32, primary_key=True)
    email = Column(types.String, unique=True, index=True)
    hashed_password = Column(types.String)
    is_active = Column(types.Boolean, default=True)

    __table_args__ = (
        engines.MergeTree(
            order_by=[literal('id')],
        ),
    )

class Location(Base):
    """class representing the location table"""
    __tablename__ = "raw_depcode"

    geo_point_2d = Column(types.String)
    geo_shape = Column(types.String)
    reg_name = Column(types.String)
    reg_code = Column(types.String)
    dep_name_upper = Column(types.String)
    dep_current_code = Column(types.String, primary_key=True)
    dep_status = Column(types.String)

    __table_args__ = (
        engines.MergeTree(
            order_by=[literal('dep_current_code')],
        ),
    )

mart_newdata = Table(
    'mart_newdata',
    metadata,
    Column("record_id", types.String),
    Column("dates", types.Date),
    Column("datetimeEpoch", types.Float64, nullable=True),
    Column("weekday_name", types.String),
    Column("department", types.String, nullable=True), # we must change it to not nullable
    Column("dep_name", types.String),
    Column("dep_code", types.String),
    Column("dep_status", types.String),
    Column("reg_name", types.String),
    Column("reg_code", types.String),
    Column("geo_point_2d", types.String),
    Column("geojson", types.String),
    Column("locations", types.String, nullable=True), # we must change it to not nullable
    Column("latitude", types.Float64, nullable=True),
    Column("longitude", types.Float64, nullable=True),
    Column("solarenergy_kwhpm2", types.Float64, nullable=True),
    Column("solarradiation", types.Float64, nullable=True),
    Column("uvindex", types.Float64, nullable=True),
    Column("temp", types.Float64, nullable=True),
    Column("tempmax", types.Float64, nullable=True),
    Column("tempmin", types.Float64, nullable=True),
    Column("feelslike", types.Float64, nullable=True),
    Column("feelslikemax", types.Float64, nullable=True),
    Column("feelslikemin", types.Float64, nullable=True),
    Column("precip", types.Float64, nullable=True),
    Column("precipprob", types.Float64, nullable=True),
    Column("precipcover", types.Float64, nullable=True),
    Column("snow", types.Float64, nullable=True),
    Column("snowdepth_filled", types.Float64, nullable=True),
    Column("dew", types.Float64, nullable=True),
    Column("humidity", types.Float64, nullable=True),
    Column("winddir", types.Float64, nullable=True),
    Column("windspeed", types.Float64, nullable=True),
    Column("windgust", types.Float64, nullable=True),
    Column("pressure", types.Float64, nullable=True),
    Column("severerisk", types.Float64, nullable=True),
    Column("icon", types.String, nullable=True),
    Column("cloudcover", types.Float64, nullable=True),
    Column("conditions", types.String, nullable=True),
    Column("moonphase", types.Float64, nullable=True),
    Column("moonphase_label", types.String),
    Column("descriptions", types.String, nullable=True),
    Column("sunrise", types.String, nullable=True),
    Column("sunset", types.String, nullable=True),
    Column("source", types.String, nullable=True),
    Column("sunriseEpoch", types.Float64, nullable=True),
    Column("sunsetEpoch", types.Float64, nullable=True),
    Column("dep_distance", types.UInt64, nullable=True), # we have to study dep_distance type
    Column("avg_solarenergy_kwhpm2", types.Float64, nullable=True),
    Column("avg_solarradiation", types.Float64, nullable=True),
    engines.MergeTree(
        order_by=[literal('dates'), literal('department')],
    )
)
