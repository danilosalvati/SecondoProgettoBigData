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

LOAD DATA LOCAL INPATH '../../actors-formatted' OVERWRITE INTO TABLE actorsRAW;

CREATE TABLE IF NOT EXISTS actors (actor STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE actors
SELECT actor, filmArray
FROM (SELECT actor, split(rowFilm,'<\\$>') as filmArray FROM actorsRAW) as actors2;

DROP TABLE actorsRaw;

--
-- Carico la tabella delle attrici --
--

CREATE TABLE IF NOT EXISTS actressesRAW (actress STRING, rowFilm STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '../../actresses-formatted' OVERWRITE INTO TABLE actressesRAW;

CREATE TABLE IF NOT EXISTS actresses (actress STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE actresses
SELECT actress, filmArray
FROM (SELECT actress, split(rowFilm,'<\\$>') as filmArray FROM actressesRAW) as actresses2;

DROP TABLE actressesRAW;

--
-- Estraggo le nazioni di produzione dei film --
--

CREATE TABLE IF NOT EXISTS countries (title STRING, nation STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '../../countries-formatted' OVERWRITE INTO TABLE countries;

--
-- Carico la tabella dei registi --
--

CREATE TABLE IF NOT EXISTS directorsRAW (director STRING, rowFilm STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '../../directors-formatted' OVERWRITE INTO TABLE directorsRAW;

CREATE TABLE IF NOT EXISTS directors (director STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE directors
SELECT director, filmArray
FROM (SELECT director, split(rowFilm,'<\\$>') as filmArray FROM directorsRAW) as directors2;

DROP TABLE directorsRAW;

--
-- Estraggo i film --
--

CREATE TABLE IF NOT EXISTS movies (title STRING, year STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '../../movies-formatted' OVERWRITE INTO TABLE movies;

--
-- Carico la tabella dei produttori --
--

CREATE TABLE IF NOT EXISTS producersRAW (producer STRING, rowFilm STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '../../producers-formatted' OVERWRITE INTO TABLE producersRAW;

CREATE TABLE IF NOT EXISTS producers (producer STRING, filmArray ARRAY<STRING>);

INSERT INTO TABLE producers
SELECT producer, filmArray
FROM (SELECT producer, split(rowFilm,'<\\$>') as filmArray FROM producersRAW) as producers2;

DROP TABLE producersRAW;

--
-- Carico la tabella delle citazioni --
--

CREATE TABLE IF NOT EXISTS quotesRAW (film STRING, rowQuotes STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '../../quotes-formatted' OVERWRITE INTO TABLE quotesRAW;

CREATE TABLE IF NOT EXISTS quotes (film STRING, quotesArray ARRAY<STRING>);

INSERT INTO TABLE quotes
SELECT film, quotesArray
FROM (SELECT film, split(rowQuotes,'<\\$>') as quotesArray FROM quotesRAW) as quotes2;

DROP TABLE quotesRaw;

--
-- Carico la tabella dei ratings --
--

CREATE TABLE IF NOT EXISTS ratingsRAW (row STRING);

LOAD DATA LOCAL INPATH '../../ratings' OVERWRITE INTO TABLE ratingsRAW;

CREATE VIEW IF NOT EXISTS ratingsArray (new, rowArray)
AS SELECT row, rowArray
FROM (SELECT row, split(row,'  ') as rowArray FROM ratingsRAW) as ratingsTmp;

CREATE TABLE IF NOT EXISTS ratings (title STRING, rank FLOAT);

INSERT INTO TABLE ratings
SELECT title, rank
FROM (SELECT rowArray[6] as title, substr(rowArray[5], 2, length(rowArray[5]) - 1) as rank FROM ratingsArray) as ratingsTmp;

DROP TABLE ratingsRAW;
DROP VIEW ratingsArray;



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

INSERT OVERWRITE LOCAL DIRECTORY 'Result/ProlificActors'
SELECT name, size(filmArray) as numFilm
FROM all_actors
SORT BY numFilm DESC
LIMIT 10;

--
-- Adesso seleziono l'anno piu' prolifico --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/ProlificYears'
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

INSERT OVERWRITE LOCAL DIRECTORY 'Result/BestActors'
SELECT name, COUNT(*) as numFilms
FROM all_actors, ratings
WHERE array_contains_substring(title,filmArray)
GROUP BY name;

--
-- Faccio la stessa cosa per i registi --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/BestDirectors'
SELECT director, COUNT(*) as numFilms
FROM directors, ratings
WHERE array_contains_substring(title,filmArray)
GROUP BY director;

--
-- Estraggo le nazioni che compaiono tra i miglior film di sempre --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/BestCountries'
SELECT nation, COUNT(*) as numFilms
FROM ratings, countries
WHERE ratings.title=countries.title
GROUP BY nation
ORDER BY numFilms DESC;

--
-- Estraggo i film di successo per ogni produttore --
--

INSERT OVERWRITE LOCAL DIRECTORY 'Result/BestProducers'
SELECT producer, COUNT(*) as numFilms
FROM ratings, producers
WHERE array_contains_substring(title,filmArray)
GROUP BY producer;

--
-- Numero di film prodotti per ciascun anno in ogni nazione --
--
INSERT OVERWRITE LOCAL DIRECTORY 'Result/FilmPerYearNations'
SELECT nation, year, COUNT(*) as numFilms
FROM movies, countries
WHERE movies.title=countries.title
GROUP BY nation, year;

--
-- Numero di citazioni per i migiori film di sempre --
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

DROP VIEW all_actors;
DROP TABLE actors;
DROP TABLE actresses;
DROP TABLE countries;
DROP TABLE directors;
DROP TABLE movies;
DROP TABLE producers;
DROP TABLE quotes;
DROP TABLE ratings;