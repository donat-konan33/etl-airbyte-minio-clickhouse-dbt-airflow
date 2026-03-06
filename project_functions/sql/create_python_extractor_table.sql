-- raw_weather_ table creating to intergate data extracted with python
-- this sql query is recommended to copy and paste in the database in order to create the table

CREATE TABLE IF NOT EXISTS raw_weather_ (
    id String,

    resolvedAddress String,
    address String,

    latitude Float64,
    longitude Float64,

    datetime String,
    datetimeEpoch Int64,

    tempmax Float64,
    tempmin Float64,
    temp Float64,

    feelslikemax Float64,
    feelslikemin Float64,
    feelslike Float64,

    dew Float64,
    humidity Float64,

    precip Float64,
    precipprob Float64,
    precipcover Float64,
    preciptype String,

    snow Float64,
    snowdepth Float64,

    windgust Float64,
    windspeed Float64,
    winddir Float64,

    pressure Float64,
    cloudcover Float64,
    visibility Float64,

    solarradiation Float64,
    solarenergy Float64,
    uvindex Float64,

    severerisk Float64,

    sunrise String,
    sunriseEpoch Int64,

    sunset String,
    sunsetEpoch Int64,

    moonphase Float64,

    conditions String,
    description String,
    icon String,

    stations Nullable(String),
    source String,

    department String
)
ENGINE = MergeTree
ORDER BY (id, department, datetime);
