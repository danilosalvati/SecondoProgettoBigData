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
hdfs dfs -get Result PIGRESULTSSINGLEANALYSISDATA
# Creo il file zip con il risultato
echo "Creating zip file"
zip -r PIGRESULTSINGLEANALYSIS.zip Result PIGRESULTSSINGLEANALYSISDATA
echo "Transferring files on s3"
aws s3 cp PIGRESULTSINGLEANALYSIS.zip s3://bigmetabucket/IMDb/
echo "Done."