#!/bin/bash

# Make sure we have a fresh start
hdfs dfs -rm -r /user/w205/hospital_compare 
rm -r ex1_data fields.csv hospital_compare


# Get the data fresh
wget -O ex1_data.zip 'https://data.medicare.gov/views/bg9k-emty/files/Nqcy71p9Ss2RSBWDmP77H1DQXcyacr2khotGbDHHW_s?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip'


# Prepare a directory for the data
mkdir ex1_data

# Unzip the files to the directory
unzip ex1_data.zip -d ex1_data/ && rm ex1_data.zip 

# Get rid of the disgusting spaces in filenames
# Will redo as while loop if I can get it to work
cd ex1_data
for i in 1 2 3 4 5 6 7
do
	rename " " "_" *
done

# Keep record of headers for files
# Then strip headers out
for f in *csv
do
	echo "\"$f\",$(head -n 1 $f)" >> ../fields.csv
	tail -n +2 $f > tmp.csv
	mv tmp.csv $f
done

# Move relevant data to separate directories for Hive
mkdir -p hospital_compare/{hospitals,effective_care,readmissions,measures,survey_responses}
cp Hospital_General_Information.csv hospital_compare/hospitals/
cp Timely_and_Effective_Care_-_Hospital.csv hospital_compare/effective_care/
cp Readmissions_and_Deaths_-_Hospital.csv hospital_compare/readmissions/
cp Measure_Dates.csv hospital_compare/measures/
cp hvbp_hcahps_05_28_2015.csv hospital_compare/survey_responses/

#sed -i 's/"[0-9]+ out of [0-9]+"/,/g' hospital_compare/survey_responses/hvbp_hcahps_05_28_2015.csv 

mv hospital_compare ../
cd -

hdfs dfs -put hospital_compare /user/w205/
hive -f hive_base_ddl.sql
