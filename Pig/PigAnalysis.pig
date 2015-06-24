/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/

/* Carico gli attori */
--actorsRAW = LOAD '../../actors-formatted' AS (actor: chararray, rowFilm: chararray);
actorsRAW = LOAD '../../xaa' AS (actor: chararray, rowFilm: chararray);
actors = FOREACH actorsRAW GENERATE actor, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/* Carico le attrici*/
--actressesRAW = LOAD '../../actresses-formatted' AS (actress: chararray, rowFilm: chararray);
actressesRAW = LOAD '../../xab' AS (actress: chararray, rowFilm: chararray);
actresses = FOREACH actressesRAW GENERATE actress, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/* Unisco attori e attrici */
allActors = UNION actors, actresses;

/* Carico i film */
movies = LOAD '../../movies-formatted' AS (title: chararray, year: chararray);

/* Carico i 250 film piu' importanti */
ratings = LOAD '../../top250films.list2' AS (distribution: chararray, votes: chararray, rank: chararray, title: chararray);

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/

/* Estraggo gli attori e le attrici piu' prolifici */
--allActorsWithNumFilms = FOREACH allActors GENERATE $0 as name, SIZE($1) as numFilm;
--allActorsWithNumFilmsSorted = ORDER allActorsWithNumFilms BY numFilm desc;
--prolificActors = LIMIT allActorsWithNumFilmsSorted 10;

--store prolificActors into 'Result/ProlificActors';

/* Trovo l'anno piu' prolifico */
--moviesGroupedByYear = GROUP movies BY year;
--allYearsWithNumFilms = FOREACH moviesGroupedByYear GENERATE group, COUNT(movies) as numFilms;
--yearsSortedByNumFilms = ORDER allYearsWithNumFilms BY numFilms desc;
--prolificYear = LIMIT yearsSortedByNumFilms 1;

--store prolificYear into 'Result/ProlificYear';

/* Trovo gli attori e le attrici che hanno partecipato ai 250 film piu' importanti */
REGISTER PigArrayContainsSubstringUDF.jar;
onlyBestMovies = FOREACH ratings GENERATE $3;
allCombinationsActorsAndMovies = CROSS onlyBestMovies,allActors;
actorsInBestMoviesWithFilms = FILTER allCombinationsActorsAndMovies BY it.uniroma3.bigDataProject.ArrayContainsSubstring($0,$2);
actorsInBestMoviesGrouped = GROUP actorsInBestMoviesWithFilms BY $1;
actorsInBestMovies = FOREACH actorsInBestMoviesGrouped GENERATE $0, SIZE($1);

store actorsInBestMovies into 'Result/BestActors';