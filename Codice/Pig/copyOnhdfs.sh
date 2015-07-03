#! /bin/bash

# Copio tutti i file su hdfs (cosi' risolvo il problema della cancellazione da parte di hive)
hdfs dfs -put ~/dataset/actorsENDVALUE.list /input
hdfs dfs -put ~/dataset/actressesENDVALUE.list /input
hdfs dfs -put ~/dataset/countriesENDVALUE.list /input
hdfs dfs -put ~/dataset/directorsENDVALUE.list /input
hdfs dfs -put ~/dataset/genresENDVALUE.list /input
hdfs dfs -put ~/dataset/keywordsENDVALUE.list /input
hdfs dfs -put ~/dataset/moviesENDVALUE.list /input
hdfs dfs -put ~/dataset/producersENDVALUE.list /input
hdfs dfs -put ~/dataset/quotesENDVALUE.list /input
hdfs dfs -put ~/dataset/top250movies.list /input