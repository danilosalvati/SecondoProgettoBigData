#! /bin/bash
echo
echo "**************************************************************************"
echo "******************* Starting Best Directors Analysis *********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/BestDirectors
{ time hive -f BestDirectors.hql ; } 2> Result/BestDirectors/tmp.txt 
cat Result/BestDirectors/tmp.txt | tail -3 > Result/BestDirectors/HiveBestDirectorsTime.txt
rm Result/BestDirectors/tmp.txt
echo "Done."