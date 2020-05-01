from kafka import KafkaProducer
import tweepy
import sys
from json import dumps


def fetchKafkaProducer():
    producerOfKafka = None
    try:
        producerOfKafka = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                  value_serializer=lambda x: dumps(x).encode('utf-8'))
    except Exception as exception:
        print("Error in connecting to kafka")
        print(str(exception))
    finally:
        return producerOfKafka


class StreamListener(tweepy.StreamListener):
    producerOfKafka = fetchKafkaProducer()

    def on_status(self, status):
        if status.lang == 'en':
            if hasattr(status, "extended_tweet"):
                remainingtxt = status.extended_tweet["full_text"]
            else:
                remainingtxt = status.text
            print(remainingtxt)
            self.producerOfKafka.send(str(sys.argv[6]), value=remainingtxt)


if __name__ == "__main__":
    # complete authorization and initialize API endpoint
    argumentsLength = len(sys.argv)
    if (argumentsLength != 7):
        print()
        print("Usage: <script_name> <consumer key> <consumer secret> <access key> <access secret> <Comma separated "
              "tweet keywords> <kafka topic name>") 
        print()
        exit(1)

    taggedTweets = str(sys.argv[5])
    clearedListByFilter = taggedTweets.split(',')

    auth = tweepy.OAuthHandler(str(sys.argv[1]), str(sys.argv[2]))
    auth.set_access_token(str(sys.argv[3]), str(sys.argv[4]))
    api = tweepy.API(auth)

    # initialize stream
    streamListener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=streamListener, tweet_mode='extended')
    stream.filter(track=clearedListByFilter)

