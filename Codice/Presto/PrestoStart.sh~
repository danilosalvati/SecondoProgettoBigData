#! /bin/bash
echo
echo "**************************************************************************"
echo "************************* Starting Presto analysis *************************"
echo "**************************************************************************"
echo

##############################################################################
#### Questo script si occupa di avviare tutti gli script di Presto        ####
#### salvando poi il risultato su s3					  ####
##############################################################################

# Creo la cartella che conterra' tutti i risultati
mkdir Result

# Avvio gli script
./startAllHiveAnalysis.sh
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

# Creo il file zip con il risultato
echo "Creating zip file"
zip -r PRESTORESULT.zip Result
echo "Transferring files on s3"
aws s3 cp PRESTORESULT.zip s3://bigmetabucket/IMDb/
echo "Done."
