/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/

/* Carico i film */
movies = LOAD 'hdfs:/input/moviesENDVALUE.list' AS (title: chararray, year: chararray);

/* Carico le nazioni */
countries = LOAD 'hdfs:/input/countriesENDVALUE.list' AS (title:chararray, nation: chararray);

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/

/* Trovo i film prodotti ogni anno per ciascuna nazione */
moviesAndCountries = JOIN movies BY title, countries BY title;
moviesAndCountriesProj = FOREACH moviesAndCountries GENERATE $0 as title, $1 as year, $3 as nation;
moviesAndCountriesGrouped = GROUP moviesAndCountriesProj BY (nation,year);
filmPerYearPerNations = FOREACH moviesAndCountriesGrouped GENERATE $0, SIZE($1);
store filmPerYearPerNations into 'Result/FilmPerYearNations';