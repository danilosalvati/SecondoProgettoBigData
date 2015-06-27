#! /bin/bash
echo
echo "**************************************************************************"
echo "*************** Starting Best BestMoviesQuotes Analysis ******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
#./copyOnhdfs.sh
mkdir Result/BestMoviesQuotes
mkdir Result/Times
mkdir Result/Times/BestMoviesQuotes
{ time pig BestMoviesQuotes.pig ; } 2> Result/Times/BestMoviesQuotes/tmp.txt 
cat Result/Times/BestMoviesQuotes/tmp.txt | tail -3 > Result/Times/PigBestMoviesQuotesTime.txt
rm -rf Result/Times/BestMoviesQuotes
echo "Done."