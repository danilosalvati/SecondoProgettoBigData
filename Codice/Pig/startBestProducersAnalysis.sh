#! /bin/bash
echo
echo "**************************************************************************"
echo "****************** Starting Best Producers Analysis **********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
#./copyOnhdfs.sh
mkdir Result/BestProducers
mkdir Result/Times
mkdir Result/Times/BestProducers
{ time pig BestProducers.pig ; } 2> Result/Times/BestProducers/tmp.txt 
cat Result/Times/BestProducers/tmp.txt | tail -3 > Result/Times/PigBestProducersTime.txt
rm -rf Result/Times/BestProducers
echo "Done."