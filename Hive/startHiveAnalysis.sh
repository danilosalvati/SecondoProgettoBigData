#! /bin/bash
echo
echo "**************************************************************************"
echo "************************* Starting hive analysis *************************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result
{ time hive -f hiveAnalysis.hql ; } 2> Result/tmp.txt 
cat Result/tmp.txt | tail -3 > Result/HiveTime.txt
rm Result/tmp.txt