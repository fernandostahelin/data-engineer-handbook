CREATE TABLE IF NOT EXISTS actors_history_scd (
    actor text,
    actorid text,
    quality_class quality_class [],
    asofyear integer,
    is_active boolean,
    end_date integer,
    start_date integer
);
