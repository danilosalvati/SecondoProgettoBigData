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
-- Estraggo le nazioni di produzione dei film --
--

CREATE TABLE IF NOT EXISTS countries (title STRING, nation STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/countriesENDVALUE.list' OVERWRITE INTO TABLE countries;

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
-- Estraggo i film --
--

CREATE TABLE IF NOT EXISTS movies (title STRING, year STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/moviesENDVALUE.list' OVERWRITE INTO TABLE movies;

--
-- Carico la tabella dei produttori --
--

CREATE TABLE IF NOT EXISTS producersRAW (producer STRING, rowFilm STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/producersENDVALUE.list' OVERWRITE INTO TABLE producersRAW;

CREATE TABLE IF NOT EXISTS producers (producer STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE producers
SELECT producer, filmArray
FROM (SELECT producer, split(rowFilm,'<ENDVALUE>') as filmArray FROM producersRAW) as producers2;

DROP TABLE producersRAW;

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

--
-- Carico la tabella delle keywords
--

CREATE TABLE IF NOT EXISTS keywords (film STRING, keyword STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/keywordsENDVALUE.list' OVERWRITE INTO TABLE keywords;

--
-- Carico la tabella dei generi --
--

CREATE TABLE IF NOT EXISTS genres (film STRING, genre STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/input/genresENDVALUE.list' OVERWRITE INTO TABLE genres;


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
-- Da questa tabella seleziono i primi 10 --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/ProlificActors'
SELECT name, size(filmArray) as numFilm
FROM all_actors
ORDER BY numFilm DESC
LIMIT 10;

--
-- Adesso seleziono l'anno piu' prolifico --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/ProlificYears'
SELECT year, COUNT(*) as numFilms
FROM movies
GROUP BY year
ORDER BY numFilms DESC
LIMIT 1;

--
-- Adesso seleziono gli attori (e le attrici) che compaiono nei primi 250 film --
--

add jar ArrayContainsSubstringUDF.jar;
CREATE TEMPORARY FUNCTION array_contains_substring AS 'it.uniroma3.bigDataProject.ArrayContainsSubstringUDF';

INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/BestActors'
SELECT name, COUNT(*) as numFilms
FROM all_actors, ratings
WHERE array_contains_substring(title,filmArray)
GROUP BY name
ORDER BY numFilms DESC;

--
-- Faccio la stessa cosa per i registi --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/BestDirectors'
SELECT director, COUNT(*) as numFilms
FROM directors, ratings
WHERE array_contains_substring(title,filmArray)
GROUP BY director
ORDER BY numFilms DESC;

--
-- Estraggo le nazioni che compaiono tra i miglior film di sempre --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/BestCountries'
SELECT nation, COUNT(*) as numFilms
FROM ratings, countries
WHERE ratings.title=countries.title
GROUP BY nation
ORDER BY numFilms DESC;

--
-- Estraggo i film di successo per ogni produttore --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/BestProducers'
SELECT producer, COUNT(*) as numFilms
FROM ratings, producers
WHERE array_contains_substring(title,filmArray)
GROUP BY producer
ORDER BY numFilms DESC;

--
-- Numero di film prodotti per ciascun anno in ogni nazione --
--
INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/FilmPerYearNations'
SELECT nation, year, COUNT(*) as numFilms
FROM movies, countries
WHERE movies.title=countries.title
GROUP BY nation, year;

--
-- Numero di citazioni per i migliori film di sempre --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/BestMoviesQuotes'
SELECT film, (size(quotesArray)-1) as numQuotes
FROM ratings,quotes
WHERE ratings.title=quotes.film
ORDER BY numQuotes DESC;


--
-- Le prime 100 Keywords per i migliori film di sempre --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/BestMoviesKeywords'
SELECT keyword, COUNT(*) as numFilms
FROM ratings,keywords
WHERE ratings.title=keywords.film
GROUP BY keyword
ORDER BY numFilms DESC
LIMIT 100;

--
-- Generi dei miglior film di sempre --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/AllAnalysis/BestMoviesGenres'
SELECT genre, COUNT(*) as numFilms
FROM ratings,genres
WHERE ratings.title=genres.film
GROUP BY genre
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
DROP TABLE countries;
DROP TABLE directors;
DROP TABLE movies;
DROP TABLE producers;
DROP TABLE quotes;
DROP TABLE ratings;
DROP TABLE keywords;
DROP TABLE genres;