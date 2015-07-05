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
hive -e 'show tables'|xargs -I '{}' hive -e 'drop table {}'
{ time sh -c 'hive -f ProlificYearsLoad.hql ; ./presto --server localhost:8080 --catalog hive --schema default --execute "SELECT year, COUNT(*) as numFilms FROM movies GROUP BY year ORDER BY numFilms DESC LIMIT 1;" --output-format CSV > Result/ProlificYears/ProlificYears.csv' ; } 2> Result/Times/ProlificYears/tmp.txt 
cat Result/Times/ProlificYears/tmp.txt | tail -3 > Result/Times/PrestoProlificYearsTime.txt
rm -rf Result/Times/ProlificYears
echo "Done."
