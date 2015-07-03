#! /bin/bash
echo
echo "**************************************************************************"
echo "*********************** Starting all pig analysis ************************"
echo "**************************************************************************"
echo

mkdir Result/AllAnalysis
{ time pig PigAnalysis.pig ; } 2> Result/AllAnalysis/tmp.txt 
cat Result/AllAnalysis/tmp.txt | tail -3 > Result/AllAnalysis/PigAllAnalysisTime.txt
rm Result/AllAnalysis/tmp.txt
echo "Done."