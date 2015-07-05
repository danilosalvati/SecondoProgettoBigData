#! /bin/bash
echo
echo "**************************************************************************"
echo "************** Starting Film Per Year And Nation Analysis ****************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result
mkdir Result/FilmPerYearNations
mkdir Result/Times
mkdir Result/Times/FilmPerYearNations
hive -e 'show tables'|xargs -I '{}' hive -e 'drop table {}'
{ time sh -c 'hive -f FilmPerYearNationsLoad.hql ; ./presto --server localhost:8080 --catalog hive --schema default --execute "SELECT nation, year, COUNT(*) as numFilms FROM movies CROSS JOIN countries WHERE movies.title=countries.title GROUP BY nation, year;" --output-format CSV > Result/FilmPerYearNations/FilmPerYearNations.csv' ; } 2> Result/Times/FilmPerYearNations/tmp.txt 
cat Result/Times/FilmPerYearNations/tmp.txt | tail -3 > Result/Times/PrestoFilmPerYearNationsTime.txt
rm -rf Result/Times/FilmPerYearNations
echo "Done."
