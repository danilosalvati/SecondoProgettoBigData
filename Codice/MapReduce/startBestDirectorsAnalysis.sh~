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
hive -e 'show tables'|xargs -I '{}' hive -e 'drop table {}'
{ time sh -c 'hive -f BestDirectorsLoad.hql ; ./presto --server localhost:8080 --catalog hive --schema default --execute "SELECT director, COUNT(*) as numFilms FROM directors CROSS JOIN ratings WHERE array_contains_substring(title,filmArray) GROUP BY director ORDER BY numFilms DESC;" --output-format CSV > Result/BestDirectors/BestDirectors.csv' ; } 2> Result/Times/BestDirectors/tmp.txt 
cat Result/Times/BestDirectors/tmp.txt | tail -3 > Result/Times/PrestoBestDirectorsTime.txt
rm -rf Result/Times/BestDirectors
echo "Done."
