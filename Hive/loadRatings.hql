CREATE TABLE IF NOT EXISTS ratingsRAW (row STRING);

LOAD DATA LOCAL INPATH '../../ratings' OVERWRITE INTO TABLE ratingsRAW;

CREATE VIEW IF NOT EXISTS ratingsArray (new, rowArray)
AS SELECT row, rowArray
FROM (SELECT row, split(row,'  ') as rowArray FROM ratingsRAW) as ratingsTmp;

CREATE TABLE IF NOT EXISTS ratings (title STRING, rank FLOAT);

INSERT INTO TABLE ratings
SELECT title, rank
FROM (SELECT rowArray[6] as title, substr(rowArray[5], 2, length(rowArray[5]) - 1) as rank FROM ratingsArray) as ratingsTmp;

-- Elimino le tabelle di supporto create --
DROP TABLE ratingsRAW;
DROP VIEW ratingsArray;
