Zach Ingbretsen Exercise 2

# Preparing the system

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

If you have not already set up this drive, you must follow the instructions from lab 2 to set up the drive:
1. Make a directory at the top level called /data
  a. Type: `ls /`

    i. Notice the "data" directory. This is where we will mount the disk.

    ii. We will always try to work out of `/data` in order to preserve data between
        sessions.

  b. Type: `chmod a+rwx /data`

    i. This sets `/data` as readable, writable and executable by all users. It is insecure, but it will eliminate permissions problems.

2. Download the setup script

  a. Type: `wget https://s3.amazonaws.com/ucbdatasciencew205/setup_ucb_complete_plus_postgres.sh`

  b. Type: `chmod +x ./setup_ucb_complete_plus_postgres.sh`

3. Run the setup script

  a. Type: `./setup_ucb_complete_plus_postgres.sh <*the device path from step 2*>`

  b. Hit Enter


# Getting the repository

Once you have the drive mounted and postgres running, create a folder in data for the exercise and download or clone the repository there.
```bash
mkdir /data/labs/
cd /data/labs/
git clone -b submission --single-branch https://github.com/zingbretsenucb/w205-sprint-17-labs-exercises
cd w205-sprint-17-labs-exercises/exercise_2
```

# Credentials

Next, please enter your twitter app's credentials in the twitter_credentials.config file. The Storm spout will not work without these credentials.

Now, you can run:

```bash
./setup.sh
```

This will install tweepy and psycopg2, move your credentials where they need to be, and create the database and table that we will be using.

If you would like to test to make sure the credentials and Tweepy are working, you can run:
```bash
python2.7 hello-stream-twitter.py
```

You should see a summary output after a few seconds.

# Running the tweet stream program

You can start the tweet stream parser by running:
```bash
cd extweetwordcount
sparse run
```

Press enter when prompted.

Press ctrl + c to stop the process after you have collected your desired amount of data.

# Querying results

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

To see more options, run:
```bash
python2.7 finalresults.py --help
```


To report all words between lengths m and n:
```bash
python2.7 histogram.py m,n

#e.g.,
python2.7 histogram.py 9,11
```