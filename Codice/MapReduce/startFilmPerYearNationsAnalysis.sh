#! /bin/bash
echo
echo "**************************************************************************"
echo "************** Starting Film Per Year And Nation Analysis ****************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result
mkdir Result/FilmPerYearNations
mkdir Result/Times
mkdir Result/Times/FilmPerYearNations
{ time hadoop jar MapReduce-1.0.jar MoviesPerYearPerCountry/MoviesPerYearPerCountry /input/countriesENDVALUE.list /input/moviesENDVALUE.list /outputFilmPerYearNations ; } 2> Result/Times/FilmPerYearNations/tmp.txt 
cat Result/Times/FilmPerYearNations/tmp.txt | tail -3 > Result/Times/MapReduceFilmPerYearNationsTime.txt
rm -rf Result/Times/FilmPerYearNations
echo "Done."
