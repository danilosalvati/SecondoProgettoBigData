---------------------------------------
------- Tutte le analisi su hive ------
---------------------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

----------------------- Caricamento delle tabelle -----------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
SET hive.execution.engine=tez;
--
-- Carico la tabella dei registi --
--

CREATE TABLE IF NOT EXISTS directorsRAW (director STRING, rowFilm STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/directorsENDVALUE.list' OVERWRITE INTO TABLE directorsRAW;

CREATE TABLE IF NOT EXISTS directors (director STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE directors
SELECT director, filmArray
FROM (SELECT director, split(rowFilm,'<ENDVALUE>') as filmArray FROM directorsRAW) as directors2;

DROP TABLE directorsRAW;

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
-- Faccio la stessa cosa per i registi --
--

add jar ArrayContainsSubstringUDF.jar;
CREATE TEMPORARY FUNCTION array_contains_substring AS 'it.uniroma3.bigDataProject.ArrayContainsSubstringUDF';

INSERT OVERWRITE LOCAL DIRECTORY 'Result/BestDirectors'
SELECT director, COUNT(*) as numFilms
FROM directors, ratings
WHERE array_contains_substring(title,filmArray)
GROUP BY director
ORDER BY numFilms DESC;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

------------------------- Eliminazione delle tabelle --------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

DROP TABLE directors;
DROP TABLE ratings;