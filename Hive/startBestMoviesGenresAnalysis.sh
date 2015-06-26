#! /bin/bash
echo
echo "**************************************************************************"
echo "****************** Starting Best MoviesGenres Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/BestMoviesGenres
{ time hive -f BestMoviesGenres.hql ; } 2> Result/BestMoviesGenres/tmp.txt 
cat Result/BestMoviesGenres/tmp.txt | tail -3 > Result/BestMoviesGenres/HiveBestMoviesGenresTime.txt
rm Result/BestMoviesGenres/tmp.txt
echo "Done."