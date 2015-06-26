#! /bin/bash
echo
echo "**************************************************************************"
echo "********************* Starting Prolific Years Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/ProlificYears
{ time hive -f ProlificYears.hql ; } 2> Result/ProlificYears/tmp.txt 
cat Result/ProlificYears/tmp.txt | tail -3 > Result/ProlificYears/HiveProlificYearsTime.txt
rm Result/ProlificYears/tmp.txt
echo "Done."