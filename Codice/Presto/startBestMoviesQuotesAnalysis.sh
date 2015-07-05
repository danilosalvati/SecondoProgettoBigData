#! /bin/bash
echo
echo "**************************************************************************"
echo "*************** Starting Best BestMoviesQuotes Analysis ******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result
mkdir Result/BestMoviesQuotes
mkdir Result/Times
mkdir Result/Times/BestMoviesQuotes
hive -e 'show tables'|xargs -I '{}' hive -e 'drop table {}'
{ time sh -c 'hive -f BestMoviesQuotesLoad.hql ; ./presto --server localhost:8080 --catalog hive --schema default --execute "SELECT film, (cardinality(quotesArray)-1) as numQuotes FROM ratings CROSS JOIN quotes WHERE ratings.title=quotes.film ORDER BY numQuotes DESC;" --output-format CSV > Result/BestMoviesQuotes/BestMoviesQuotes.csv' ; } 2> Result/Times/BestMoviesQuotes/tmp.txt 
cat Result/Times/BestMoviesQuotes/tmp.txt | tail -3 > Result/Times/PrestoBestMoviesQuotesTime.txt
rm -rf Result/Times/BestMoviesQuotes
echo "Done."
