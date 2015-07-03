#! /bin/bash
echo
echo "**************************************************************************"
echo "*********************** Starting All Tez Analysis ************************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
./copyOnhdfs.sh
mkdir Result/AllAnalysis
{ time hive -f tezAnalysis.hql ; } 2> Result/AllAnalysis/tmp.txt 
cat Result/AllAnalysis/tmp.txt | tail -3 > Result/AllAnalysis/TezAllAnalysisTime.txt
rm Result/AllAnalysis/tmp.txt
echo "Done."