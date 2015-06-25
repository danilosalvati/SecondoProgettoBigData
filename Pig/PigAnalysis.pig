/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/

/* Carico gli attori */
actorsRAW = LOAD '../../actors-formatted' AS (actor: chararray, rowFilm: chararray);
actors = FOREACH actorsRAW GENERATE actor, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/* Carico le attrici*/
actressesRAW = LOAD '../../actresses-formatted' AS (actress: chararray, rowFilm: chararray);
actresses = FOREACH actressesRAW GENERATE actress, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/* Unisco attori e attrici */
allActors = UNION actors, actresses;

/* Carico i film */
movies = LOAD '../../movies-formatted' AS (title: chararray, year: chararray);

/* Carico i 250 film piu' importanti */
ratings = LOAD '../../top250films.list' AS (distribution: chararray, votes: chararray, rank: chararray, title: chararray);

/* Carico i registi */
directorsRAW = LOAD '../../directors-formatted' AS (director: chararray, rowFilm: chararray);
directors = FOREACH directorsRAW GENERATE director, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/* Carico i produttori */
producersRAW = LOAD '../../producers-formatted' AS (producer: chararray, rowFilm: chararray);
producers = FOREACH producersRAW GENERATE producer, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/* Carico le nazioni */
countries = LOAD '../../countries-formatted' AS (title:chararray, nation: chararray);

/* Carico le citazioni */
quotesRAW = LOAD '../../quotes-formatted' AS (film: chararray, rowQuotes: chararray);
quotes = FOREACH quotesRAW GENERATE film, STRSPLIT(rowQuotes, '<ENDVALUE>', 0)  as quotesArray;

/* Carico le keywords */
keywords = LOAD '../../keywords-formatted' AS (film:chararray, keyword: chararray);

/* Carico i generi */
genres = LOAD '../../genres-formatted' AS (film:chararray, genre: chararray);

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/

/* Estraggo gli attori e le attrici piu' prolifici */
allActorsWithNumFilms = FOREACH allActors GENERATE $0 as name, SIZE($1) as numFilm;
allActorsWithNumFilmsSorted = ORDER allActorsWithNumFilms BY numFilm desc;
prolificActors = LIMIT allActorsWithNumFilmsSorted 10;
store prolificActors into 'Result/ProlificActors';

/* Trovo l'anno piu' prolifico */
moviesGroupedByYear = GROUP movies BY year;
allYearsWithNumFilms = FOREACH moviesGroupedByYear GENERATE group, COUNT(movies) as numFilms;
yearsSortedByNumFilms = ORDER allYearsWithNumFilms BY numFilms desc;
prolificYear = LIMIT yearsSortedByNumFilms 1;
store prolificYear into 'Result/ProlificYear';

/* Trovo gli attori e le attrici che hanno partecipato ai 250 film piu' importanti */
REGISTER PigArrayContainsSubstringUDF.jar;
onlyBestMovies = FOREACH ratings GENERATE $3 as movieTitle;
allCombinationsActorsAndMovies = CROSS onlyBestMovies,allActors;
actorsInBestMoviesWithFilms = FILTER allCombinationsActorsAndMovies BY it.uniroma3.bigDataProject.ArrayContainsSubstring($0,$2);
actorsInBestMoviesGrouped = GROUP actorsInBestMoviesWithFilms BY $1;
actorsInBestMovies = FOREACH actorsInBestMoviesGrouped GENERATE $0, SIZE($1);
store actorsInBestMovies into 'Result/BestActors';

/* Trovo i registi che hanno partecipato ai 250 film piu' importanti */
allCombinationsDirectorsAndMovies = CROSS onlyBestMovies,directors;
directorsInBestMoviesWithFilms = FILTER allCombinationsDirectorsAndMovies BY it.uniroma3.bigDataProject.ArrayContainsSubstring($0,$2);
directorsInBestMoviesGrouped = GROUP directorsInBestMoviesWithFilms BY $1;
directorsInBestMovies = FOREACH directorsInBestMoviesGrouped GENERATE $0, SIZE($1);
store actorsInBestMovies into 'Result/BestDirectors';

/* Trovo le nazioni dei film piu' importanti di sempre */
bestFilmsAndCountries = JOIN onlyBestMovies BY movieTitle, countries BY title;
titleAndCountries = FOREACH bestFilmsAndCountries GENERATE $0 as title, $2 as nation;
bestFilmsAndCountriesGrouped = GROUP bestFilmsAndCountries BY nation;
bestCountries = FOREACH bestFilmsAndCountriesGrouped GENERATE $0 as country, SIZE($1) as numFilms;
bestCountriesSorted = ORDER bestCountries BY numFilms desc;
store bestCountriesSorted into 'Result/BestCountries';

/* Trovo i produttori dei film piu' importanti */
allCombinationsProducersAndMovies = CROSS onlyBestMovies,producers;
producersInBestMoviesWithFilms = FILTER allCombinationsProducersAndMovies BY it.uniroma3.bigDataProject.ArrayContainsSubstring($0,$2);
producersInBestMoviesGrouped = GROUP producersInBestMoviesWithFilms BY $1;
producersInBestMovies = FOREACH producersInBestMoviesGrouped GENERATE $0, SIZE($1);
store producersInBestMovies into 'Result/BestProducers';

/* Trovo i film prodotti ogni anno per ciascuna nazione */
moviesAndCountries = JOIN movies BY title, countries BY title;
moviesAndCountriesProj = FOREACH moviesAndCountries GENERATE $0 as title, $1 as year, $3 as nation;
moviesAndCountriesGrouped = GROUP moviesAndCountriesProj BY (nation,year);
filmPerYearPerNations = FOREACH moviesAndCountriesGrouped GENERATE $0, SIZE($1);
store filmPerYearPerNations into 'Result/FilmPerYearNations';

/* Trovo il numero di citazioni per i migliori film di sempre */
bestMoviesAndQuotes = JOIN onlyBestMovies BY movieTitle, quotes BY film;
bestMoviesQuotesWithSize = FOREACH bestMoviesAndQuotes GENERATE $0 as film, SIZE($2) as numQuotes;
bestMoviesQuotes = ORDER bestMoviesQuotesWithSize BY numQuotes desc;
store bestMoviesQuotes into 'Result/BestMoviesQuotes';

/* Trovo le prime 100 keywords per i migliori film di sempre */
bestMoviesAndKeywords = JOIN onlyBestMovies BY movieTitle, keywords BY film;
bestMoviesKeywordsGrouped = GROUP bestMoviesAndKeywords BY $2;
bestMoviesKeywordsWithSize = FOREACH bestMoviesKeywordsGrouped GENERATE $0,SIZE($1) as numFilms;
bestMoviesKeywordsOrdered = ORDER bestMoviesKeywordsWithSize BY numFilms desc;
bestMoviesKeywords = LIMIT bestMoviesKeywordsOrdered 100;
store bestMoviesKeywords into 'Result/BestMoviesKeywords';

/* Trovo i generi dei miglior film di sempre */
bestMoviesAndGenres = JOIN onlyBestMovies BY movieTitle, genres BY film;
bestMoviesGenresGrouped = GROUP bestMoviesAndGenres BY $2;
bestMoviesGenresWithSize = FOREACH bestMoviesGenresGrouped GENERATE $0,SIZE($1) as numFilms;
bestMoviesGenresOrdered = ORDER bestMoviesGenresWithSize BY numFilms desc;
store bestMoviesGenres into 'Result/BestMoviesGenres';