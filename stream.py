from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from api_tokens import access_token, access_token_secret, consumer_key, consumer_secret

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status, file=sys.stderr)

if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(languages=['fr'], track=['a', 'e', 'i', 'o', 'u', 'y', 'é', 'â', 'ê', 'î', 'ô', 'û' 'à', 'è', 'ù', 'ë', 'ï', 'ü'])
