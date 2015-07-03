#! /bin/bash
echo
echo "**************************************************************************"
echo "************************ Starting Spark analysis *************************"
echo "**************************************************************************"
echo

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
zip -r SPARKRESULTSINGLEANALYSIS.zip Result
echo "Transferring files on s3"
aws s3 cp SPARKRESULTSINGLEANALYSIS.zip s3://bigmetabucket/IMDb/
echo "Done."