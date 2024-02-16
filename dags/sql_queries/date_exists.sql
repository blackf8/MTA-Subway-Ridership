SELECT exists (
    SELECT 1
    FROM $table_name
    where $col::date = '$date'
    LIMIT 1
);