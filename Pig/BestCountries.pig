/**************************************************************************************
***************************************************************************************

********************************* CARICAMENTO DEI DATI ********************************

***************************************************************************************
***************************************************************************************/

/* Carico i 250 film piu' importanti */
ratings = LOAD 'hdfs:/input/top250movies.list' AS (distribution: chararray, votes: chararray, rank: chararray, title: chararray);

/* Carico le nazioni */
countries = LOAD 'hdfs:/input/countriesENDVALUE.list' AS (title:chararray, nation: chararray);

/**************************************************************************************
***************************************************************************************

*********************************** ANALISI DEI DATI **********************************

***************************************************************************************
***************************************************************************************/

onlyBestMovies = FOREACH ratings GENERATE $3 as movieTitle;

/* Trovo le nazioni dei film piu' importanti di sempre */
bestFilmsAndCountries = JOIN onlyBestMovies BY movieTitle, countries BY title;
titleAndCountries = FOREACH bestFilmsAndCountries GENERATE $0 as title, $2 as nation;
bestFilmsAndCountriesGrouped = GROUP bestFilmsAndCountries BY nation;
bestCountries = FOREACH bestFilmsAndCountriesGrouped GENERATE $0 as country, SIZE($1) as numFilms;
bestCountriesSorted = ORDER bestCountries BY numFilms desc;
store bestCountriesSorted into 'Result/BestCountries';