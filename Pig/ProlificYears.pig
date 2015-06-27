/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/

/* Carico i film */
movies = LOAD 'hdfs:/input/moviesENDVALUE.list' AS (title: chararray, year: chararray);

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/

/* Trovo l'anno piu' prolifico */
moviesGroupedByYear = GROUP movies BY year;
allYearsWithNumFilms = FOREACH moviesGroupedByYear GENERATE group, COUNT(movies) as numFilms;
yearsSortedByNumFilms = ORDER allYearsWithNumFilms BY numFilms desc;
prolificYear = LIMIT yearsSortedByNumFilms 1;
store prolificYear into 'Result/ProlificYears';