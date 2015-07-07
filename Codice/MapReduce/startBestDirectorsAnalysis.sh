#! /bin/bash
echo
echo "**************************************************************************"
echo "******************* Starting Best Directors Analysis *********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result
mkdir Result/BestDirectors
mkdir Result/Times
mkdir Result/Times/BestDirectors
{ time hadoop jar MapReduce-1.0.jar Directors250TopMovies/Directors250TopMovies /input/directorsENDVALUE.list /outputBestDirectors ; } 2> Result/Times/BestDirectors/tmp.txt 
cat Result/Times/BestDirectors/tmp.txt | tail -3 > Result/Times/MapReduceBestDirectorsTime.txt
rm -rf Result/Times/BestDirectors
echo "Done."
