#! /bin/bash
echo
echo "**************************************************************************"
echo "****************** Starting Best Producers Analysis **********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result
mkdir Result/BestProducers
mkdir Result/Times
mkdir Result/Times/BestProducers
hive -e 'show tables'|xargs -I '{}' hive -e 'drop table {}'
{ time sh -c 'hive -f BestProducersLoad.hql ; ./presto --server localhost:8080 --catalog hive --schema default --execute "SELECT producer, COUNT(*) as numFilms FROM ratings CROSS JOIN producers WHERE array_contains_substring(title,filmArray) GROUP BY producer ORDER BY numFilms DESC;" --output-format CSV > Result/BestProducers/BestProducers.csv' ; } 2> Result/Times/BestProducers/tmp.txt 
cat Result/Times/BestProducers/tmp.txt | tail -3 > Result/Times/PrestoBestProducersTime.txt
rm -rf Result/Times/BestProducers
echo "Done."
