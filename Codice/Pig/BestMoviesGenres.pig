/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/
/* Carico i 250 film piu' importanti */
ratings = LOAD 'hdfs:/input/top250movies.list' AS (distribution: chararray, votes: chararray, rank: chararray, title: chararray);

/* Carico i generi */
genres = LOAD 'hdfs:/input/genresENDVALUE.list' AS (film:chararray, genre: chararray);

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/

onlyBestMovies = FOREACH ratings GENERATE $3 as movieTitle;

/* Trovo i generi dei miglior film di sempre */
bestMoviesAndGenres = JOIN onlyBestMovies BY movieTitle, genres BY film;
bestMoviesGenresGrouped = GROUP bestMoviesAndGenres BY $2;
bestMoviesGenresWithSize = FOREACH bestMoviesGenresGrouped GENERATE $0,SIZE($1) as numFilms;
bestMoviesGenres = ORDER bestMoviesGenresWithSize BY numFilms desc;
store bestMoviesGenres into 'Result/BestMoviesGenres';