#!/usr/bin/env bash

# Setup for Ex2

# Install required Python packages
pip2.7 install tweepy
pip2.7 install psycopg2==2.6.2

# This was run when setting up project
# but does not need to be done again
# If there's an existing project, back it up
# mv extweetwordcount extweetwordcount.bak

# # Create new project
# sparse quickstart extweetwordcount

# # Copy files from designated project
# rsync -avz tweetwordcount/* extweetwordcount/
# cd extweetwordcount

# # Remove unnecessary files
# rm topologies/wordcount.clj
# rm virtualenvs/wordcount.txt
# rm src/spouts/words.py

# Move the user's credentials to the proper place
source setup_config.sh

# Use supplied credentials to create a new database and table with
# names supplied in credentials file
python2.7 setup_database.py
