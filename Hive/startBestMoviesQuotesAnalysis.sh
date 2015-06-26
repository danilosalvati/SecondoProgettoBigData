#! /bin/bash
echo
echo "**************************************************************************"
echo "*************** Starting Best BestMoviesQuotes Analysis ******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/BestMoviesQuotes
{ time hive -f BestMoviesQuotes.hql ; } 2> Result/BestMoviesQuotes/tmp.txt 
cat Result/BestMoviesQuotes/tmp.txt | tail -3 > Result/BestMoviesQuotes/HiveBestMoviesQuotesTime.txt
rm Result/BestMoviesQuotes/tmp.txt
echo "Done."