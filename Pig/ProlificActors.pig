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