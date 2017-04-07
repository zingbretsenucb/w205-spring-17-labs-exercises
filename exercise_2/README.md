Zach Ingbretsen Exercise 2

In order to run this, you must make sure that postgres is running and you have mounted the /data directory

If you have not done this, run the following command:

```bash
./startup.sh
```

Next, please enter your twitter app's credentials in the twitter_credentials.config file. The Storm spout will not work without these credentials.

Now, you can run:

```bash
./setup.sh
```

This will install tweepy and psycopg2, move your credentials where they need to be, and create the database and table that we will be using.


