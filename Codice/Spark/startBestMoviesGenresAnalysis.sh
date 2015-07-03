#! /bin/bash
echo
echo "**************************************************************************"
echo "****************** Starting Best MoviesGenres Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result/BestMoviesGenres
mkdir Result/Times
mkdir Result/Times/BestMoviesGenres
{ time spark-submit --class "it.uniroma3.bigDataProject.BestMoviesGenresAnalysis" --master yarn-client SparkIMDb.jar ; } 2> Result/Times/BestMoviesGenres/tmp.txt 
cat Result/Times/BestMoviesGenres/tmp.txt | tail -3 > Result/Times/SparkBestMoviesGenresTime.txt
rm -rf Result/Times/BestMoviesGenres
echo "Done."