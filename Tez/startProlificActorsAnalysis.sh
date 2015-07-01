#! /bin/bash
echo
echo "**************************************************************************"
echo "******************** Starting Prolific Actors Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result/ProlificActors
mkdir Result/Times
mkdir Result/Times/ProlificActors
{ time hive -f ProlificActors.hql ; } 2> Result/Times/ProlificActors/tmp.txt 
cat Result/Times/ProlificActors/tmp.txt | tail -3 > Result/Times/TezProlificActorsTime.txt
rm -rf Result/Times/ProlificActors
echo "Done."