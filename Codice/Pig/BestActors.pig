/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/

/* Carico gli attori */
actorsRAW = LOAD 'hdfs:/input/actorsENDVALUE.list' AS (actor: chararray, rowFilm: chararray);
actors = FOREACH actorsRAW GENERATE actor, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/* Carico le attrici*/
actressesRAW = LOAD 'hdfs:/input/actressesENDVALUE.list' AS (actress: chararray, rowFilm: chararray);
actresses = FOREACH actressesRAW GENERATE actress, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/* Unisco attori e attrici */
allActors = UNION actors, actresses;

/* Carico i 250 film piu' importanti */
ratings = LOAD 'hdfs:/input/top250movies.list' AS (distribution: chararray, votes: chararray, rank: chararray, title: chararray);

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/
/* Trovo gli attori e le attrici che hanno partecipato ai 250 film piu' importanti */
REGISTER PigArrayContainsSubstringUDF.jar;
onlyBestMovies = FOREACH ratings GENERATE $3 as movieTitle;
allCombinationsActorsAndMovies = CROSS onlyBestMovies,allActors;
actorsInBestMoviesWithFilms = FILTER allCombinationsActorsAndMovies BY it.uniroma3.bigDataProject.ArrayContainsSubstring($0,$2);
actorsInBestMoviesGrouped = GROUP actorsInBestMoviesWithFilms BY $1;
actorsInBestMovies = FOREACH actorsInBestMoviesGrouped GENERATE $0, SIZE($1);
actorsInBestMoviesOrdered = ORDER actorsInBestMovies BY $1 desc;
store actorsInBestMoviesOrdered into 'Result/BestActors';