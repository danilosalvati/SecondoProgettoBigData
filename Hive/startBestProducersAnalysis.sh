#! /bin/bash
echo
echo "**************************************************************************"
echo "****************** Starting Best Producers Analysis **********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/BestProducers
{ time hive -f BestProducers.hql ; } 2> Result/BestProducers/tmp.txt 
cat Result/BestProducers/tmp.txt | tail -3 > Result/BestProducers/HiveBestProducersTime.txt
rm Result/BestProducers/tmp.txt
echo "Done."