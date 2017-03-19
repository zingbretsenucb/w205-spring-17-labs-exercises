#!/usr/bin/env bash

mv extweetwordcount extweetwordcount.bak
sparse quickstart extweetwordcount
rsync -avz tweetwordcount/* extweetwordcount/
cd extweetwordcount
rm topologies/wordcount.clj
rm virtualenvs/wordcount.txt
rm src/spouts/words.py
