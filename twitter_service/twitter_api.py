import tweepy

METRO_VANCOUVER_GEOBOX = [-123.371556,49.009125,-122.264683,49.375294]

# Twitter API Keys
access_token = "1354933647602720781-uAAs2vePSl3spkdiFYKRz51LwNmQY0"
access_token_secret = "S4OQjwPyeJ1VHSZyyjHcvZ1WiLPOt4Yg6pdMhv0q0VIS5"
consumer_key = "BbS4k0PqSa3y80IFnhUSjOXz4"
consumer_secret = "GSE59GfdAsBwrN3S7c7s0GEuQPDTb4ilMNu0ry3iqwV0OYWVzm"


class stream_listener(tweepy.StreamListener):

    def on_status(self, status):
        print(status.text)

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False
        print('Streaming Error: '+str(status_code))


if __name__ == '__main__':
    try:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)
        api.update_status('tweepy + oauth!')
    except tweepy.TweepError as e:
        print(e)

    stream_listener = stream_listener()
    stream = tweepy.Stream(auth = api.auth, listener=stream_listener)
    stream.filter(locations=METRO_VANCOUVER_GEOBOX)