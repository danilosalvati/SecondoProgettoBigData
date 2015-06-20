-- Carico la tabella dei registi --

CREATE TABLE IF NOT EXISTS directorsRAW (director STRING, rowFilm STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '../../directors-formatted' OVERWRITE INTO TABLE directorsRAW;

CREATE TABLE IF NOT EXISTS directors (director STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE directors
SELECT director, filmArray
FROM (SELECT director, split(rowFilm,'<\\$>') as filmArray FROM directorsRAW) as directors2;

DELETE TABLE directorsRAW; 
