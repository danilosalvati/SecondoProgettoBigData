/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/

/* Carico i 250 film piu' importanti */
ratings = LOAD 'hdfs:/input/top250movies.list' AS (distribution: chararray, votes: chararray, rank: chararray, title: chararray);

/* Carico i registi */
directorsRAW = LOAD 'hdfs:/input/directorsENDVALUE.list' AS (director: chararray, rowFilm: chararray);
directors = FOREACH directorsRAW GENERATE director, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/

REGISTER PigArrayContainsSubstringUDF.jar;
onlyBestMovies = FOREACH ratings GENERATE $3 as movieTitle;

/* Trovo i registi che hanno partecipato ai 250 film piu' importanti */
allCombinationsDirectorsAndMovies = CROSS onlyBestMovies,directors;
directorsInBestMoviesWithFilms = FILTER allCombinationsDirectorsAndMovies BY it.uniroma3.bigDataProject.ArrayContainsSubstring($0,$2);
directorsInBestMoviesGrouped = GROUP directorsInBestMoviesWithFilms BY $1;
directorsInBestMovies = FOREACH directorsInBestMoviesGrouped GENERATE $0, SIZE($1);
directorsInBestMoviesOrdered = ORDER directorsInBestMovies BY $1 desc;
store directorsInBestMoviesOrdered into 'Result/BestDirectors';