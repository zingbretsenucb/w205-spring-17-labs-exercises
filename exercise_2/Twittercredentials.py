import tweepy

consumer_key = "xbdaspoSAKRZ9iv2q3In6xQJX";


consumer_secret = "eQuL4BuZ9WpKbzBkJeosjRbd5EvljjSGjKE8XrvA3nl9LcUVRA";

access_token = "474295968-VThlDa4WDRMVgJQTs8w7iyuAtFBoDk8OG9jRmbUr";

access_token_secret = "kimRtcTJh3HKu9OcbPEARYnPYet3G8J6H2DC6BVO14EAr";


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)



