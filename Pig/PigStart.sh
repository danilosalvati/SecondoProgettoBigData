#! /bin/bash
echo
echo "**************************************************************************"
echo "************************* Starting pig analysis *************************"
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
./startBestActorsAnalysis.sh
./startBestCountriesAnalysis.sh
./startBestDirectorsAnalysis.sh
./startBestMoviesGenresAnalysis.sh
./startBestMoviesKeywordsAnalysis.sh
./startBestMoviesQuotesAnalysis.sh
./startBestProducersAnalysis.sh
./startFilmPerYearNationsAnalysis.sh
./startProlificActorsAnalysis.sh
./startProlificYearsAnalysis.sh

# Prendo i file dall'hdfs
hdfs dfs -get Result PIGRESULTS
# Creo il file zip con il risultato
echo "Creating zip file"
zip -r PIGRESULT.zip Result PIGRESULTS
# Trasferisco i file su s3
echo "Transferring files on s3"
aws s3 cp PIGRESULT.zip s3://bigmetabucket/IMDb/
echo "Done."