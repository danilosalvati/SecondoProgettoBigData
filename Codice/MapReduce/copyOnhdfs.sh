#! /bin/bash

# Copio tutti i file su hdfs
hdfs dfs -put /mnt/dataset/actorsENDVALUE.list /input
hdfs dfs -put /mnt/dataset/actressesENDVALUE.list /input
hdfs dfs -put /mnt/dataset/countriesENDVALUE.list /input
hdfs dfs -put /mnt/dataset/directorsENDVALUE.list /input
hdfs dfs -put /mnt/dataset/genresENDVALUE.list /input
hdfs dfs -put /mnt/dataset/keywordsENDVALUE.list /input
hdfs dfs -put /mnt/dataset/moviesENDVALUE.list /input
hdfs dfs -put /mnt/dataset/producersENDVALUE.list /input
hdfs dfs -put /mnt/dataset/quotesENDVALUE.list /input
hdfs dfs -put /mnt/dataset/top250movies.list /input
