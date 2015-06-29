#! /bin/bash
spark-submit --class "it.uniroma3.bigDataProject.AllAnalysis" --master local[4] SparkIMDb.jar