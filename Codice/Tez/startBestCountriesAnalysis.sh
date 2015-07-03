#! /bin/bash
echo
echo "**************************************************************************"
echo "******************* Starting Best Countries Analysis *********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result/BestCountries
mkdir Result/Times
mkdir Result/Times/BestCountries
{ time hive -f BestCountries.hql ; } 2> Result/Times/BestCountries/tmp.txt 
cat Result/Times/BestCountries/tmp.txt | tail -3 > Result/Times/TezBestCountriesTime.txt
rm -rf Result/Times/BestCountries
echo "Done."