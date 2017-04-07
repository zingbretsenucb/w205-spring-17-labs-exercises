#!/usr/bin/env bash

# Move Twitter and postgres credentials where required
cp twitter_credentials.config credentials/
cp postgres_credentials.config FetchResults/
cp postgres_credentials.config extweetwordcount/src/bolts/credentials.config
cp twitter_credentials.config extweetwordcount/src/spouts/credentials.config
