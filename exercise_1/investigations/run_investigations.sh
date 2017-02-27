#!/usr/bin/env bash

# This will run the sql files for the questions and print the results along with my interpretations

hive -f  best_hospitals/investigate_hospitals.sql
cat  best_hospitals/investigate_hospitals.txt

hive -f  best_states/investigate_states.sql
cat  best_states/investigate_states.txt

hive -f  hospital_variability/investigate_variability.sql
cat  hospital_variability/investigate_variability.txt

hive -f  hospitals_and_patients/investigate_surveys.sql
cat  hospitals_and_patients/investigate_surveys.txt
