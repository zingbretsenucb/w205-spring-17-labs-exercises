#!/usr/bin/env bash

# Move Twitter and postgres credentials where required
cp twitter_credentials.config credentials/
cp postgres_credentials.config extweetwordcount/src/bolts/
cp postgres_credentials.config extweetwordcount/src/spouts/
