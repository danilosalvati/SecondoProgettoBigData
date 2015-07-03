#! /bin/bash
echo
echo "**************************************************************************"
echo "************************* Starting hive analysis *************************"
echo "**************************************************************************"
echo

##############################################################################
#### Questo script si occupa di avviare tutti gli script di hive salvando ####
#### poi il risultato su s3						  ####
##############################################################################

# Creo la cartella che conterra' tutti i risultati
mkdir Result

# Avvio gli script
./startAllHiveAnalysis.sh

# Creo il file zip con il risultato
echo "Creating zip file"
zip -r HIVERESULTONLYCOMPLETE.zip Result
echo "Transferring files on s3"
aws s3 cp HIVERESULTONLYCOMPLETE.zip s3://bigmetabucket/IMDb/
echo "Done."