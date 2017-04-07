Zach Ingbretsen Exercise 2

In order to run this, you must make sure that postgres is running and you have mounted the /data directory

If you have not done this, run the following command (you may need to change the name of the drive you're mounting):

```bash
./startup.sh
```

Next, please enter your twitter app's credentials in the twitter_credentials.config file. The Storm spout will not work without these credentials.

Now, you can run:

```bash
./setup.sh
```

This will install tweepy and psycopg2, move your credentials where they need to be, and create the database and table that we will be using.


You can run the tweet stream parser by running:
```bash
cd extweetwordcount
sparse run
```

Press enter when prompted

Press ctrl + c to stop the process


Once you have collected some data, you may run the following commands to inspect the data.

To print all words and their counts:
```bash
python2.7 finalresults.py
```

To print a given word and its count:
```bash
python2.7 finalresults.py <word>

#e.g.,
python2.7 finalresults.py tweet
```

To report all words between lengths m and n:
```bash
python2.7 histogram.py m,n

#e.g.,
python2.7 histogram.py 9,11
```