#! /bin/bash
echo
echo "**************************************************************************"
echo "************************* Starting tez analysis **************************"
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

# Creo il file zip con il risultato
echo "Creating zip file"
zip -r TEZRESULTSINGLEANALYSIS.zip Result
echo "Transferring files on s3"
aws s3 cp TEZRESULTSINGLEANALYSIS.zip s3://bigmetabucket/IMDb/
echo "Done."