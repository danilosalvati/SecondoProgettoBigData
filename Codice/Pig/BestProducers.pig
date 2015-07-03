/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/
/* Carico i 250 film piu' importanti */
ratings = LOAD 'hdfs:/input/top250movies.list' AS (distribution: chararray, votes: chararray, rank: chararray, title: chararray);

/* Carico i produttori */
producersRAW = LOAD 'hdfs:/input/producersENDVALUE.list' AS (producer: chararray, rowFilm: chararray);
producers = FOREACH producersRAW GENERATE producer, STRSPLIT(rowFilm, '<ENDVALUE>', 0)  as filmArray;

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/

REGISTER PigArrayContainsSubstringUDF.jar;
onlyBestMovies = FOREACH ratings GENERATE $3 as movieTitle;

/* Trovo i produttori dei film piu' importanti */
allCombinationsProducersAndMovies = CROSS onlyBestMovies,producers;
producersInBestMoviesWithFilms = FILTER allCombinationsProducersAndMovies BY it.uniroma3.bigDataProject.ArrayContainsSubstring($0,$2);
producersInBestMoviesGrouped = GROUP producersInBestMoviesWithFilms BY $1;
producersInBestMovies = FOREACH producersInBestMoviesGrouped GENERATE $0, SIZE($1);
producersInBestMoviesOrdered = ORDER producersInBestMovies BY $1 desc;
store producersInBestMoviesOrdered into 'Result/BestProducers';