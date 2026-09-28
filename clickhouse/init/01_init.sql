-- create database before creating tables
CREATE DATABASE IF NOT EXISTS datawarehouse;

-- select the database to use
USE datawarehouse;

-- create raw_depcode_ table
CREATE TABLE IF NOT EXISTS raw_depcode_ (
    geo_point_2d String,        -- often encoded to WKT, WKB or GeoJSON
    geo_shape String,           -- often encoded to WKT, WKB or GeoJSON
    reg_name String,
    reg_code String,
    dep_name_upper String,
    dep_current_code String,
    dep_status Nullable(String),     -- may be needed to be Nullable(String) here to allow NULL values because of existing likewise NULL values for dep_status
    department String,
    dep_normalized String
)
ENGINE = MergeTree()
ORDER BY dep_current_code;

--create raw_weather_ table
CREATE TABLE IF NOT EXISTS raw_weather_ (
    id String,

    resolvedAddress String,
    address String,

    latitude Float64,
    longitude Float64,

    datetime String,
    datetimeEpoch Int64,

    tempmax Nullable(Float64),
    tempmin Nullable(Float64),
    temp Nullable(Float64),

    feelslikemax Nullable(Float64),
    feelslikemin Nullable(Float64),
    feelslike Nullable(Float64),

    dew Nullable(Float64),
    humidity Nullable(Float64),

    precip Nullable(Float64),
    precipprob Nullable(Float64),
    precipcover Nullable(Float64),
    preciptype Nullable(String),

    snow Nullable(Float64),
    snowdepth Nullable(Float64),

    windgust Nullable(Float64),
    windspeed Nullable(Float64),
    winddir Nullable(Float64),

    pressure Nullable(Float64),
    cloudcover Nullable(Float64),
    visibility Nullable(Float64),

    solarradiation Nullable(Float64),
    solarenergy Nullable(Float64),
    uvindex Nullable(Float64),

    severerisk Nullable(Float64),

    sunrise Nullable(String),
    sunriseEpoch Nullable(Int64),

    sunset Nullable(String),
    sunsetEpoch Nullable(Int64),

    moonphase Nullable(Float64),

    conditions Nullable(String),
    description Nullable(String),
    icon Nullable(String),

    stations Nullable(String),
    source Nullable(String),

    department String
)
ENGINE = MergeTree
ORDER BY (id, department, datetime);
