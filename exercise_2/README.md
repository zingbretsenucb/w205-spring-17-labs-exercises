Zach Ingbretsen Exercise 2

In order to run this, you must make sure that postgres is running and you have mounted the /data directory.

If you have not done this, run the following commands as root (you may need to change the name of the drive you're mounting):

```bash
mount -t ext4 /dev/xvdf /data
```

If you are unable to mount the drive, make sure 
1) It is attached (via the AWS EC2 Dashboard)
2) The drive is in the location you think it is. Try running the following to find your attached volume's location: 
```bash
fdisk -l
```

If you have already set up this volumne to run postgres, you can now simply run:
```bash
/data/start_postgres.sh
```

If you have not already set up this drive, you must follow the instructions from lab XXX to set up the drive.

Once you have the drive mounted and postgres running, create a folder in data for the exercise and download or clone the repository there.
```bash
mkdir /data/labs/
cd /data/labs/
git clone -b submission --single-branch https://github.com/zingbretsenucb/w205-sprint-17-labs-exercises
cd w205-sprint-17-labs-exercises/exercise_2
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

Press enter when prompted.

Press ctrl + c to stop the process.


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
