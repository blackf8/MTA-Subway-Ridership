CREATE TABLE IF NOT EXISTS $table_name(
    transit_timestamp TIMESTAMPTZ,
    historical VARCHAR(20),
    wifi_provided VARCHAR(20),
    daily_ridership_count Integer
);