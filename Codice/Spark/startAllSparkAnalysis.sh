#! /bin/bash
echo
echo "**************************************************************************"
echo "********************** Starting All Spark Analysis ***********************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/AllAnalysis
{ time spark-submit --class "it.uniroma3.bigDataProject.AllAnalysis" --master yarn-client SparkIMDb.jar ; } 2> Result/AllAnalysis/tmp.txt 
cat Result/AllAnalysis/tmp.txt | tail -3 > Result/AllAnalysis/SparkAllAnalysisTime.txt
rm Result/AllAnalysis/tmp.txt
echo "Done."