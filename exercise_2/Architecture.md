# Zach Ingbretsen Exercise 2

For instructions on how to run the code, please see README.md!

# Purpose

Being able to process streaming data is becoming increasingly important as the amount and velocity of data being collected by companies and individuals surge. Processing these data (e.g., Twitter data) in more traditional ways is likened to trying to drink from a firehose. Specialized stream processing is required to process these data in such a way as to derive insights quickly and respond to changes in realtime.

The idea behind this project is to demonstrate a streaming processing pipeline of Twitter data. This project implements a simple word counter to parse tweets into machine-readable words, count the occurrances of each of those words, and then store those counts in a database. This framework coule easily be expanded to process and filter data for business purposes.

# Architecture

The backbone of this project is Apache Storm. Apache Storm is stream processing software that is module and robust to 


## Relevant directories and files



.
├── Architecture.md
├── Architecture.pdf
├── credentials
│   └── Twittercredentials.py
├── extweetwordcount
│   ├── config.json
│   ├── example.config
│   ├── fabfile.py
│   ├── project.clj
│   ├── README.md
│   ├── src
│   │   ├── bolts
│   │   │   ├── parse.py
│   │   │   ├── postgres
│   │   │   │   └── postgresUpdater.py~
│   │   │   └── wordcount.py
│   │   └── spouts
│   │       └── tweets.py
│   ├── tasks.py
│   ├── topologies
│   │   └── tweetwordcount.clj
│   └── virtualenvs
│       └── tweetwordcount.txt
├── FetchResults
│   ├── FetchResults.py
├── finalresults.py
├── hello-stream-twitter.py
├── histogram.py
├── postgres_credentials.config
├── README.md
├── setup_config.sh
├── setup_database.py
├── setup.sh
├── startup.sh
├── twitter_credentials.config
