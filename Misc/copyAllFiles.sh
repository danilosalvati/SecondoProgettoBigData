#! /bin/bash
echo
echo "**************************************************************************"
echo "************************* Copying files from s3 **************************"
echo "**************************************************************************"
echo
mkdir ~/dataset
aws s3 cp  s3://bigmetabucket/IMDb/Edited/actorsENDVALUE.list ~/dataset/actorsENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/actressesENDVALUE.list ~/dataset/actressesENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/countriesENDVALUE.list ~/dataset/countriesENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/directorsENDVALUE.list ~/dataset/directorsENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/genresENDVALUE.list ~/dataset/genresENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/keywordsENDVALUE.list ~/dataset/keywordsENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/moviesENDVALUE.list ~/dataset/moviesENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/producersENDVALUE.list ~/dataset/producersENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/quotesENDVALUE.list ~/dataset/quotesENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/top250movies.list ~/dataset/top250movies.list

echo
echo "**************************************************************************"
echo "************************* Copying files on hdfs **************************"
echo "**************************************************************************"
echo

hdfs dfs -mkdir /input
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