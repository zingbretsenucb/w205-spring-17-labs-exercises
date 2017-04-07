#!/usr/bin/env bash

# Setup for Ex2

# Install required Python packages
pip2.7 install tweepy
pip2.7 install psycopg2==2.6.2

# Move the user's credentials to the proper place
source setup_config.sh

# Use supplied credentials to create a new database and table with
# names supplied in credentials file
python2.7 setup_database.py
