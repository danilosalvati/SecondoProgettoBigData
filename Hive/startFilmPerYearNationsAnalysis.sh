#! /bin/bash
echo
echo "**************************************************************************"
echo "************** Starting Film Per Year And Nation Analysis ****************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/FilmPerYearNations
{ time hive -f FilmPerYearNations.hql ; } 2> Result/FilmPerYearNations/tmp.txt 
cat Result/FilmPerYearNations/tmp.txt | tail -3 > Result/FilmPerYearNations/HiveFilmPerYearNationsTime.txt
rm Result/FilmPerYearNations/tmp.txt
echo "Done."