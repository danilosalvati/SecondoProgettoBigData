---------------------------------------
---------- BestMoviesQuotes -----------
---------------------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

----------------------- Caricamento delle tabelle -----------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

--
-- Carico la tabella delle citazioni --
--

CREATE TABLE IF NOT EXISTS quotesRAW (film STRING, rowQuotes STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/quotesENDVALUE.list' OVERWRITE INTO TABLE quotesRAW;

CREATE TABLE IF NOT EXISTS quotes (film STRING, quotesArray ARRAY<STRING>);

INSERT INTO TABLE quotes
SELECT film, quotesArray
FROM (SELECT film, split(rowQuotes,'<ENDVALUE>') as quotesArray FROM quotesRAW) as quotes2;

DROP TABLE quotesRaw;

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
-- Numero di citazioni per i migliori film di sempre --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/BestMoviesQuotes'
SELECT film, size(quotesArray) as numQuotes
FROM ratings,quotes
WHERE ratings.title=quotes.film
SORT BY numQuotes DESC;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

------------------------- Eliminazione delle tabelle --------------------------

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

DROP TABLE quotes;
DROP TABLE ratings;