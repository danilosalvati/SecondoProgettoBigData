#! /bin/bash
echo
echo "**************************************************************************"
echo "************** Starting Best BestMoviesKeywords Analysis *****************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
#./copyOnhdfs.sh
mkdir Result/BestMoviesKeywords
mkdir Result/Times
mkdir Result/Times/BestMoviesKeywords
{ time pig BestMoviesKeywords.pig ; } 2> Result/Times/BestMoviesKeywords/tmp.txt 
cat Result/Times/BestMoviesKeywords/tmp.txt | tail -3 > Result/Times/PigBestMoviesKeywordsTime.txt
rm -rf Result/Times/BestMoviesKeywords/
echo "Done."