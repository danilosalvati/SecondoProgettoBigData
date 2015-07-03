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
./startAllPigAnalysis.sh

# Prendo i file dall'hdfs
hdfs dfs -get Result PIGRESULTSONLYCOMPLETEDATA
# Creo il file zip con il risultato
echo "Creating zip file"
zip -r PIGRESULTONLYCOMPLETE.zip Result PIGRESULTSONLYCOMPLETEDATA
echo "Transferring files on s3"
aws s3 cp PIGRESULTONLYCOMPLETE.zip s3://bigmetabucket/IMDb/
echo "Done."