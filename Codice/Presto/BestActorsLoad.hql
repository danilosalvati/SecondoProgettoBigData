-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

----------------------- Caricamento delle tabelle -----------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

--
-- Carico la tabella degli attori --
--

CREATE TABLE IF NOT EXISTS actorsRAW (actor STRING, rowFilm STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/actorsENDVALUE.list' OVERWRITE INTO TABLE actorsRAW;

CREATE TABLE IF NOT EXISTS actors (actor STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE actors
SELECT actor, filmArray
FROM (SELECT actor, split(rowFilm,'<ENDVALUE>') as filmArray FROM actorsRAW) as actors2;

DROP TABLE actorsRaw;

--
-- Carico la tabella delle attrici --
--

CREATE TABLE IF NOT EXISTS actressesRAW (actress STRING, rowFilm STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/actressesENDVALUE.list' OVERWRITE INTO TABLE actressesRAW;

CREATE TABLE IF NOT EXISTS actresses (actress STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE actresses
SELECT actress, filmArray
FROM (SELECT actress, split(rowFilm,'<ENDVALUE>') as filmArray FROM actressesRAW) as actresses2;

DROP TABLE actressesRAW;

--
-- Carico la tabella dei ratings --
--

CREATE TABLE IF NOT EXISTS ratings (distribution STRING, votes STRING, rank STRING, title STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
    
LOAD DATA INPATH '/input/top250movies.list' OVERWRITE INTO TABLE ratings;
