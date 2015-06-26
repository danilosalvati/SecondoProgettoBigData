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

--
-- Estraggo le nazioni di produzione dei film --
--

CREATE TABLE IF NOT EXISTS countries (title STRING, nation STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/countriesENDVALUE.list' OVERWRITE INTO TABLE countries;

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
-- Estraggo le nazioni che compaiono tra i miglior film di sempre --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/BestCountries'
SELECT nation, COUNT(*) as numFilms
FROM ratings, countries
WHERE ratings.title=countries.title
GROUP BY nation
ORDER BY numFilms DESC;


-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

------------------------- Eliminazione delle tabelle --------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

DROP TABLE countries;
DROP TABLE ratings;