#!bin/env pyspark

from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType, DoubleType

from pyspark import SparkContext
from pyspark.sql import HiveContext
sc = SparkContext()
sq = HiveContext(sc)

def convertToFrac(mystr):
    nums = mystr.split(' out of ')
    return float(nums[0]) / float(nums[1])

udf = UserDefinedFunction(lambda x: convertToFrac(x), DoubleType())

def convertRowToFrac(row):
    outrow = []
    for x in row:
	try:
	    outrow.append(convertToFrac(x))
	except:
	    outrow.append(x)
    return outrow

def ident(x):
    return x

results = sq.sql('SELECT * FROM survey_responses limit 5')

cols = [
'Communication_Nurses_Achievement_Points',
'Communication_Nurses_Improvement_Points',
'Communication_Nurses_Dimension_Score',
'Communication_Doctors_Achievement_Points',
'Communication_Doctors_Improvement_Points',
'Communication_Doctors_Dimension_Score',
'Responsiveness_of_Hospital_Staff_Achievement_Points',
'Responsiveness_of_Hospital_Staff_Improvement_Points',
'Responsiveness_of_Hospital_Staff_Dimension_Score',
'Pain_Management_Achievement_Points',
'Pain_Management_Improvement_Points',
'Pain_Management_Dimension_Score',
'Communication_about_Medicines_Achievement_Points',
'Communication_about_Medicines_Improvement_Points',
'Communication_about_Medicines_Dimension_Score',
'Cleanliness_and_Quietness_of_Hospital_Environment_Achievement_Points',
'Cleanliness_and_Quietness_of_Hospital_Environment_Improvement_Points',
'Cleanliness_and_Quietness_of_Hospital_Environment_Dimension_Score',
'Discharge_Information_Achievement_Points',
'Discharge_Information_Improvement_Points',
'Discharge_Information_Dimension_Score',
'Overall_Rating_of_Hospital_Achievement_Points',
'Overall_Rating_of_Hospital_Improvement_Points',
'Overall_Rating_of_Hospital_Dimension_Score'
]
cols = [col.lower() for col in cols]

results = results.select(*[
    udf(column).alias(column)
    if column in cols else column
    for column in results.columns])

results.show()
#results.registerTempTable('tmp')
#sq.sql('DROP TABLE XXX')
#sq.sql('SELECT * FROM tmp').show()

#converted = results.map(convertRowToFrac).reduce(ident).show()
#converted.registerAsTable('tmp')
#sq.sql('CREATE TABLE XXX AS SELECT * FROM tmp')
