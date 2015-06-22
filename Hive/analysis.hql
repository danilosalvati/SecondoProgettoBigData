-- Creo alcune analisi di interesse per il mio sistema --

-- Per prima cosa metto insieme gli attori maschi e le femmine --

CREATE VIEW all_actors (name, filmArray)
AS SELECT actor as name, filmArray
FROM actors
UNION ALL
SELECT actress as name, filmArray
FROM actresses;

-- Da questa tabella seleziono i primi 10 --
SELECT name, size(filmArray) as numFilm
FROM all_actors
SORT BY numFilm DESC
LIMIT 10;

-- Adesso seleziono l'anno piu' prolifico --
SELECT year, COUNT(*) as numFilms
FROM movies
GROUP BY year
ORDER BY numFilms DESC
LIMIT 1;

-- Adesso seleziono gli attori (e le attrici) che compaiono nei primi 250 film --
add jar ArrayContainsSubstringUDF.jar;
CREATE TEMPORARY FUNCTION array_contains_substring AS 'it.uniroma3.bigDataProject.ArrayContainsSubstringUDF';

SELECT name, COUNT(*) as numFilms
FROM all_actors, ratings
WHERE array_contains_substring(title,filmArray)
GROUP BY name;

-- Faccio la stessa cosa per i registi --
SELECT name, COUNT(*) as numFilms
FROM directors, ratings
WHERE array_contains_substring(title,filmArray)
GROUP BY name;




DROP VIEW all_actors;