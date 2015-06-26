#! /bin/bash
echo
echo "**************************************************************************"
echo "******************** Starting Prolific Actors Analysis *******************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/ProlificActors
{ time hive -f ProlificActors.hql ; } 2> Result/ProlificActors/tmp.txt 
cat Result/ProlificActors/tmp.txt | tail -3 > Result/ProlificActors/HiveProlificActorsTime.txt
rm Result/ProlificActors/tmp.txt
echo "Done."