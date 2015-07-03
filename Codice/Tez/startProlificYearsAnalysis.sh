#! /bin/bash
echo
echo "**************************************************************************"
echo "********************* Starting Prolific Years Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result/ProlificYears
mkdir Result/Times
mkdir Result/Times/ProlificYears
{ time hive -f ProlificYears.hql ; } 2> Result/Times/ProlificYears/tmp.txt 
cat Result/Times/ProlificYears/tmp.txt | tail -3 > Result/Times/TezProlificYearsTime.txt
rm -rf Result/Times/ProlificYears
echo "Done."