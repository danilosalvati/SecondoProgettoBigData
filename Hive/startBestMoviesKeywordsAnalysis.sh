#! /bin/bash
echo
echo "**************************************************************************"
echo "************** Starting Best BestMoviesKeywords Analysis *****************"
echo "**************************************************************************"
echo

# Memorizzo anche il tempo impiegato dallo script salvandolo su un file apposito
mkdir Result/BestMoviesKeywords
{ time hive -f BestMoviesKeywords.hql ; } 2> Result/BestMoviesKeywords/tmp.txt 
cat Result/BestMoviesKeywords/tmp.txt | tail -3 > Result/BestMoviesKeywords/HiveBestMoviesKeywordsTime.txt
rm Result/BestMoviesKeywords/tmp.txt
echo "Done."