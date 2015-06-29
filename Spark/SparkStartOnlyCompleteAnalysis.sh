#! /bin/bash
echo
echo "**************************************************************************"
echo "************************ Starting Spark analysis *************************"
echo "**************************************************************************"
echo

# Creo la cartella che conterra' tutti i risultati
mkdir Result

# Avvio gli script
./startAllSparkAnalysis.sh

# Creo il file zip con il risultato
echo "Creating zip file"
zip -r SPARKRESULTONLYCOMPLETE.zip Result
echo "Transferring files on s3"
aws s3 cp SPARKRESULTONLYCOMPLETE.zip s3://bigmetabucket/IMDb/
echo "Done."