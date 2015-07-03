#! /bin/bash
echo
echo "**************************************************************************"
echo "****************** Starting Best MoviesGenres Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
#./copyOnhdfs.sh
mkdir Result/BestMoviesGenres
mkdir Result/Times
mkdir Result/Times/BestMoviesGenres
{ time pig BestMoviesGenres.pig ; } 2> Result/Times/BestMoviesGenres/tmp.txt 
cat Result/Times/BestMoviesGenres/tmp.txt | tail -3 > Result/Times/PigBestMoviesGenresTime.txt
rm -rf Result/Times/BestMoviesGenres
echo "Done."