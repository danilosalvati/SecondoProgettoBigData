#! /bin/bash
echo
echo "**************************************************************************"
echo "************************* Starting pig analysis **************************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result
{ time pig PigAnalysis.pig; } 2> Result/tmp.txt 
cat Result/tmp.txt | tail -3 > Result/PigTime.txt
rm Result/tmp.txt
# Copio i risultati su s3
aws s3 cp Result s3://bigmetabucket/IMDb/PIGRESULT/ --recursive
# Creo lo zip
zip -r PIGRESULT.zip Result
aws s3 cp PIGRESULT.zip s3://bigmetabucket/IMDb/