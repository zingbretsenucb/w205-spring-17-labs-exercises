DROP TABLE hospitals;
CREATE EXTERNAL TABLE hospitals (
Provider_ID STRING,
Hospital_Name STRING,
Address STRING,
City STRING,
State STRING,
ZIP_Code STRING,
County_Name STRING,
Phone_Number STRING,
Hospital_Type STRING,
Hospital_Ownership STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
		"separatorChar" = ",",
		"quoteChar" = '"',
		"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/hospitals';

DROP TABLE effective_care;
CREATE EXTERNAL TABLE effective_care (
Provider_ID STRING,
Hospital_Name STRING,
Address STRING,
City STRING,
State STRING,
ZIP_Code STRING,
County_Name STRING,
Phone_Number STRING,
Conditions STRING,
Measure_ID STRING,
Measure_Name STRING,
Score STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
		"separatorChar" = ",",
		"quoteChar" = '"',
		"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/effective_care';



DROP TABLE readmissions;
CREATE EXTERNAL TABLE readmissions (
Provider_ID STRING,
Hospital_Name STRING,
Address STRING,
City STRING,
State STRING,
ZIP_Code STRING,
County_Name STRING,
Phone_Number STRING,
Measure_Name STRING,
Measure_ID STRING,
Compared_to_National STRING,
Denominator STRING,
Score STRING,
Lower_Estimate STRING,
Higher_Estimate STRING,
Footnote STRING,
Measure_Start_Date STRING,
Measure_End_Date STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
		"separatorChar" = ",",
		"quoteChar" = '"',
		"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/readmissions';


DROP TABLE measures;
CREATE EXTERNAL TABLE measures (
Measure_Name STRING,
Measure_ID STRING,
Measure_Start_Quarter STRING,
Measure_Start_Date STRING,
Measure_End_Quarter STRING,
Measure_End_Date STRING
)
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
	WITH SERDEPROPERTIES (
			"separatorChar" = ",",
			"quoteChar" = '"',
			"escapeChar" = '\\'
			)
	STORED AS TEXTFILE
	LOCATION '/user/w205/hospital_compare/measures';

DROP TABLE survey_responses;
CREATE EXTERNAL TABLE survey_responses (
Provider_ID STRING,
Hospital_Name STRING,
Address STRING,
City STRING,
State STRING,
ZIP_Code STRING,
County_Name STRING,
Communication_Nurses_Achievement_Points STRING,
Communication_Nurses_Improvement_Points STRING,
Communication_Nurses_Dimension_Score STRING,
Communication_Doctors_Achievement_Points STRING,
Communication_Doctors_Improvement_Points STRING,
Communication_Doctors_Dimension_Score STRING,
Responsiveness_of_Hospital_Staff_Achievement_Points STRING,
Responsiveness_of_Hospital_Staff_Improvement_Points STRING,
Responsiveness_of_Hospital_Staff_Dimension_Score STRING,
Pain_Management_Achievement_Points STRING,
Pain_Management_Improvement_Points STRING,
Pain_Management_Dimension_Score STRING,
Communication_meds_Achievement_Points STRING,
Communication_meds_Improvement_Points STRING,
Communication_meds_Dimension_Score STRING,
Cleanliness_Achievement_Points STRING,
Cleanliness_Improvement_Points STRING,
Cleanliness_Dimension_Score STRING,
Discharge_Achievement_Points STRING,
Discharge_Improvement_Points STRING,
Discharge_Dimension_Score STRING,
Overall_Achievement_Points STRING,
Overall_Improvement_Points STRING,
Overall_Dimension_Score STRING,
Base_Score STRING,
Consistency STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
		"separatorChar" = ",",
		"quoteChar" = '"',
		"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/survey_responses';
