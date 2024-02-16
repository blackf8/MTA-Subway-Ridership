SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_name = '$table_name'
) AS table_existence;