#! /bin/bash
echo
echo "**************************************************************************"
echo "*********************** Starting All Hive Analysis ***********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/AllAnalysis
{ time hive -f hiveAnalysis.hql ; } 2> Result/AllAnalysis/tmp.txt 
cat Result/AllAnalysis/tmp.txt | tail -3 > Result/AllAnalysis/HiveProlificYearsTime.txt
rm Result/AllAnalysis/tmp.txt
echo "Done."