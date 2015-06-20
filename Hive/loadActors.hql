-- Carico la tabella degli attori --

CREATE TABLE IF NOT EXISTS actorsRAW (actor STRING, rowFilm STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

-- LOAD DATA LOCAL INPATH '../../actors-formatted' OVERWRITE INTO TABLE actorsRAW;
LOAD DATA LOCAL INPATH '../../xaa' OVERWRITE INTO TABLE actorsRAW;

CREATE TABLE IF NOT EXISTS actors (actor STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE actors
SELECT actor, filmArray
FROM (SELECT actor, split(rowFilm,'<\\$>') as filmArray FROM actorsRAW) as actors2;

DELETE TABLE actorsRaw;