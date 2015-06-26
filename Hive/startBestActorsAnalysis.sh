#! /bin/bash
echo
echo "**************************************************************************"
echo "********************** Starting Best Actor Analysis **********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/BestActors
{ time hive -f BestActors.hql ; } 2> Result/BestActors/tmp.txt 
cat Result/BestActors/tmp.txt | tail -3 > Result/BestActors/HiveBestActorsTime.txt
rm Result/BestActors/tmp.txt
echo "Done."