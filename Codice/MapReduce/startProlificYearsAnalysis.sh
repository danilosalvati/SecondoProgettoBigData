#! /bin/bash
echo
echo "**************************************************************************"
echo "********************* Starting Prolific Years Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result
mkdir Result/ProlificYears
mkdir Result/Times
mkdir Result/Times/ProlificYears
{ time hadoop jar MapReduce-1.0.jar MostProlificYear/MostProlificYear /input/moviesENDVALUE.list /outputProlificYears ; } 2> Result/Times/ProlificYears/tmp.txt 
cat Result/Times/ProlificYears/tmp.txt | tail -3 > Result/Times/MapReduceProlificYearsTime.txt
rm -rf Result/Times/ProlificYears
echo "Done."
