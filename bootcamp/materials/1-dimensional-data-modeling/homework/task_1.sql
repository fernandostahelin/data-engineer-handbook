CREATE TYPE films AS (
    film text,
    filmid text,
    votes Integer,
    rating real
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');


CREATE TABLE IF NOT EXISTS actors (
    actor text,
    actorid text,
    asofyear integer,
    films films [],
    quality_class quality_class [],
    is_active boolean
);
