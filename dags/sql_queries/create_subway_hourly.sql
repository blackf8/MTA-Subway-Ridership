CREATE TABLE IF NOT EXISTS $table_name(
    transit_timestamp TIMESTAMPTZ,
    transit_mode VARCHAR(35),
    station_complex_id VARCHAR(20),
    station_complex VARCHAR(80),
    borough VARCHAR(30),
    payment_method VARCHAR(50),
    fare_class_category VARCHAR(50),
    ridership FLOAT,
    transfers FLOAT,
    latitude FLOAT,
    longitude FLOAT
);