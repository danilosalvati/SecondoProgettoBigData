---------------------------------------
-------------- BestActors -------------
---------------------------------------

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

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

-------------------------- Fase di analisi sui dati ---------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

--
-- Per prima cosa metto insieme gli attori maschi e le femmine --
--

CREATE VIEW all_actors (name, filmArray)
AS SELECT actor as name, filmArray
FROM actors
UNION ALL
SELECT actress as name, filmArray
FROM actresses;

--
-- Adesso seleziono gli attori (e le attrici) che compaiono nei primi 250 film --
--

add jar ArrayContainsSubstringUDF.jar;
CREATE TEMPORARY FUNCTION array_contains_substring AS 'it.uniroma3.bigDataProject.ArrayContainsSubstringUDF';

INSERT OVERWRITE LOCAL DIRECTORY 'Result/BestActors'
SELECT name, COUNT(*) as numFilms
FROM all_actors, ratings
WHERE array_contains_substring(title,filmArray)
GROUP BY name
ORDER BY numFilms DESC;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

------------------------- Eliminazione delle tabelle --------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

DROP VIEW all_actors;
DROP TABLE actors;
DROP TABLE actresses;
DROP TABLE ratings;
