/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/

/* Carico i 250 film piu' importanti */
ratings = LOAD 'hdfs:/input/top250movies.list' AS (distribution: chararray, votes: chararray, rank: chararray, title: chararray);

/* Carico le keywords */
keywords = LOAD 'hdfs:/input/keywordsENDVALUE.list' AS (film:chararray, keyword: chararray);

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/
onlyBestMovies = FOREACH ratings GENERATE $3 as movieTitle;

/* Trovo le prime 100 keywords per i migliori film di sempre */
bestMoviesAndKeywords = JOIN onlyBestMovies BY movieTitle, keywords BY film;
bestMoviesKeywordsGrouped = GROUP bestMoviesAndKeywords BY $2;
bestMoviesKeywordsWithSize = FOREACH bestMoviesKeywordsGrouped GENERATE $0,SIZE($1) as numFilms;
bestMoviesKeywordsOrdered = ORDER bestMoviesKeywordsWithSize BY numFilms desc;
bestMoviesKeywords = LIMIT bestMoviesKeywordsOrdered 100;
store bestMoviesKeywords into 'Result/BestMoviesKeywords';