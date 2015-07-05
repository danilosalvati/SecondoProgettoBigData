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
-- Estraggo i film --
--

CREATE TABLE IF NOT EXISTS movies (title STRING, year STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/moviesENDVALUE.list' OVERWRITE INTO TABLE movies;
