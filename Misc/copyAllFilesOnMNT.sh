#! /bin/bash
echo
echo "**************************************************************************"
echo "************************* Copying files from s3 **************************"
echo "**************************************************************************"
echo
mkdir /mnt/dataset
aws s3 cp  s3://bigmetabucket/IMDb/Edited/actorsENDVALUE.list /mnt/dataset/actorsENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/actressesENDVALUE.list /mnt/dataset/actressesENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/countriesENDVALUE.list /mnt/dataset/countriesENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/directorsENDVALUE.list /mnt/dataset/directorsENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/genresENDVALUE.list /mnt/dataset/genresENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/keywordsENDVALUE.list /mnt/dataset/keywordsENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/moviesENDVALUE.list /mnt/dataset/moviesENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/producersENDVALUE.list /mnt/dataset/producersENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/quotesENDVALUE.list /mnt/dataset/quotesENDVALUE.list
aws s3 cp  s3://bigmetabucket/IMDb/Edited/top250movies.list /mnt/dataset/top250movies.list

echo
echo "**************************************************************************"
echo "************************* Copying files on hdfs **************************"
echo "**************************************************************************"
echo

hdfs dfs -mkdir /input
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