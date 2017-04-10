import tweepy
import ConfigParser
import os

file_path = os.path.dirname(os.path.realpath(__file__))
parser = ConfigParser.ConfigParser()

parser.read(os.path.join(file_path, 'twitter_credentials.config'))

consumer_key        = parser.get('TWITTER', 'consumer_key');
consumer_secret     = parser.get('TWITTER', 'consumer_secret');
access_token        = parser.get('TWITTER', 'access_token');
access_token_secret = parser.get('TWITTER', 'access_token_secret');

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
