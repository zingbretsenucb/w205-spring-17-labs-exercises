#!/usr/bin/env bash

# This pulls the data from the web source and puts the data into HDFS
cd loading_and_modeling
load_data_lake.sh
cd -

# This transforms the data into a cleaner form
cd transforming
run_transformation.sh
cd -

# This will run the final SQL queries to answer 
# the questions and print my interpretations
cd investigations
run_investigations.sh
cd -
