#! /bin/bash
echo
echo "**************************************************************************"
echo "******************* Starting Best Countries Analysis *********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/BestCountries
{ time hive -f BestCountries.hql ; } 2> Result/BestCountries/tmp.txt 
cat Result/BestCountries/tmp.txt | tail -3 > Result/BestCountries/HiveBestCountriesTime.txt
rm Result/BestCountries/tmp.txt
echo "Done."