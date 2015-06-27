/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/
/* Carico i 250 film piu' importanti */
ratings = LOAD 'hdfs:/input/top250movies.list' AS (distribution: chararray, votes: chararray, rank: chararray, title: chararray);

/* Carico le citazioni */
quotesRAW = LOAD 'hdfs:/input/quotesENDVALUE.list' AS (film: chararray, rowQuotes: chararray);
quotes = FOREACH quotesRAW GENERATE film, STRSPLIT(rowQuotes, '<ENDVALUE>', 0)  as quotesArray;

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/
onlyBestMovies = FOREACH ratings GENERATE $3 as movieTitle;

/* Trovo il numero di citazioni per i migliori film di sempre */
bestMoviesAndQuotes = JOIN onlyBestMovies BY movieTitle, quotes BY film;
bestMoviesQuotesWithSize = FOREACH bestMoviesAndQuotes GENERATE $0 as film, SIZE($2) as numQuotes;
bestMoviesQuotes = ORDER bestMoviesQuotesWithSize BY numQuotes desc;
store bestMoviesQuotes into 'Result/BestMoviesQuotes';