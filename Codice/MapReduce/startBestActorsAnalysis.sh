#! /bin/bash
echo
echo "**************************************************************************"
echo "********************** Starting Best Actor Analysis **********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result
mkdir Result/BestActors
mkdir Result/Times
mkdir Result/Times/BestActors
{ time hadoop jar MapReduce-1.0.jar ActorsAndActresses250TopMovies/ActorsAndActresses250TopMovies /input/actorsENDVALUE.list /input/actressesENDVALUE.list /outputBestActors ; } 2> Result/Times/BestActors/tmp.txt 
cat Result/Times/BestActors/tmp.txt | tail -3 > Result/Times/MapReduceBestActorsTime.txt
rm -rf Result/Times/BestActors
echo "Done."
