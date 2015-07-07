#! /bin/bash
echo
echo "**************************************************************************"
echo "****************** Starting Best MoviesGenres Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result
mkdir Result/BestMoviesGenres
mkdir Result/Times
mkdir Result/Times/BestMoviesGenres
{ time hadoop jar MapReduce-1.0.jar Genres250TopMovies/Genres250TopMovies /input/genresENDVALUE.list /outputBestMovieGenres ; } 2> Result/Times/BestMoviesGenres/tmp.txt 
cat Result/Times/BestMoviesGenres/tmp.txt | tail -3 > Result/Times/MapReduceBestMoviesGenresTime.txt
rm -rf Result/Times/BestMoviesGenres
echo "Done."
