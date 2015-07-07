#! /bin/bash
echo
echo "**************************************************************************"
echo "******************** Starting Prolific Actors Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result
mkdir Result/ProlificActors
mkdir Result/Times
mkdir Result/Times/ProlificActors
hive -e 'show tables'|xargs -I '{}' hive -e 'drop table {}'
{ time sh -c 'hive -f ProlificActorsLoad.hql ; ./presto --server localhost:8080 --catalog hive --schema default --execute "WITH all_actors AS (SELECT actor as name, filmArray FROM actors UNION ALL SELECT actress as name, filmArray FROM actresses) SELECT name, cardinality(filmArray) as numFilm FROM all_actors ORDER BY numFilm DESC LIMIT 10;" --output-format CSV > Result/ProlificActors/ProlificActors.csv' ; } 2> Result/Times/ProlificActors/tmp.txt 
cat Result/Times/ProlificActors/tmp.txt | tail -3 > Result/Times/PrestoProlificActorsTime.txt
rm -rf Result/Times/ProlificActors
echo "Done."
