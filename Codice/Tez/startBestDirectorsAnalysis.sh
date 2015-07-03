#! /bin/bash
echo
echo "**************************************************************************"
echo "******************* Starting Best Directors Analysis *********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result/BestDirectors
mkdir Result/Times
mkdir Result/Times/BestDirectors
{ time hive -f BestDirectors.hql ; } 2> Result/Times/BestDirectors/tmp.txt 
cat Result/Times/BestDirectors/tmp.txt | tail -3 > Result/Times/TezBestDirectorsTime.txt
rm -rf Result/Times/BestDirectors
echo "Done."