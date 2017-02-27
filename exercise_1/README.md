Before starting the analysis, please make sure Hadoop, Postgres, and the Hive Metastore are running.

As root:
```
/root/start_hadoop.sh
/data/start_postgres.sh
su - w205
```

As w205:
```
/data/start_metastore.sh
```

You can then run all the scripts for the analysis with the command:
```
sh run_all
```

This will run the three steps of the analysis:
1) Load in all the data from the web source, add it to HDFS and Hive
2) Transform the data via pyspark to only contain the columns of interest
3) Run the final analyses and print out the interpretations

Please see the investigations directory for text files of my analysis.
