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
hive -e 'show tables'|xargs -I '{}' hive -e 'drop table {}'
{ time sh -c 'hive -f BestActorsLoad.hql ; ./presto --server localhost:8080 --catalog hive --schema default --execute "WITH all_actors AS (SELECT actor as name, filmArray FROM actors UNION ALL SELECT actress as name, filmArray FROM actresses) SELECT name, COUNT(*) as numFilms FROM all_actors CROSS JOIN ratings WHERE array_contains_substring(title,filmArray) GROUP BY name ORDER BY numFilms DESC;" --output-format CSV > Result/BestActors/BestActors.csv' ; } 2> Result/Times/BestActors/tmp.txt 
cat Result/Times/BestActors/tmp.txt | tail -3 > Result/Times/PrestoBestActorsTime.txt
rm -rf Result/Times/BestActors
echo "Done."
