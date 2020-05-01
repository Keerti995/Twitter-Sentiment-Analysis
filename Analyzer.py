from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import twitter_samples, stopwords
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk import FreqDist, classify, NaiveBayesClassifier
import json
import hashlib
from kafka import KafkaConsumer
import re, string, random
from elasticsearch import Elasticsearch

def remove_disturbance(tweet_tokens, stop_words=()):
    cleared_tokens = []

    for token, tag in pos_tag(tweet_tokens):
        token = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|' \
                       '(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', token)
        token = re.sub("(@[A-Za-z0-9_]+)", "", token)

        if tag.startswith("NN"):
            pos = 'n'
        elif tag.startswith('VB'):
            pos = 'v'
        else:
            pos = 'a'

        wordnetlemmatizer = WordNetLemmatizer()
        token = wordnetlemmatizer.lemmatize(token, pos)

        if len(token) > 0 and token not in string.punctuation and token.lower() not in stop_words:
            cleared_tokens.append(token.lower())
    return cleared_tokens


def fetch_all_words(cleared_tokens_list):
    for tkns in cleared_tokens_list:
        for tkn in tkns:
            yield tkn


def fetch_tweets_for_model(cleared_tokens_list):
    for tkns_tweets in cleared_tokens_list:
        yield dict([tkn, True] for tkn in tkns_tweets)


def fetchSentimentAnalyzer():
    halting_words = stopwords.words('english')
    pos_tkns = twitter_samples.tokenized('positive_tweets.json')
    neg_tkns = twitter_samples.tokenized('negative_tweets.json')

    pos_cleaned_tkns_list = []
    neg_cleaned_tkns_list = []

    for tkns in pos_tkns:
        pos_cleaned_tkns_list.append(remove_disturbance(tkns, halting_words))

    for tkns in neg_tkns:
        neg_cleaned_tkns_list.append(remove_disturbance(tkns, halting_words))

    all_pos_words = fetch_all_words(pos_cleaned_tkns_list)

    freq_dist_pos = FreqDist(all_pos_words)
    print(freq_dist_pos.most_common(10))

    pos_tkns_for_model = fetch_tweets_for_model(pos_cleaned_tkns_list)
    neg_tkns_for_model = fetch_tweets_for_model(neg_cleaned_tkns_list)

    pos_DS = [(tweet_dict, "Positive")
                        for tweet_dict in pos_tkns_for_model]

    neg_DS = [(tweet_dict, "Negative")
                        for tweet_dict in neg_tkns_for_model]

    bothDS = pos_DS + neg_DS
    random.shuffle(bothDS)
    train_data = bothDS
    classifier = NaiveBayesClassifier.train(train_data)
    return classifier


def addId(data):
    j = json.dumps(data).encode('ascii', 'ignore')
    data['doc_id'] = hashlib.sha224(j).hexdigest()
    return (data['doc_id'], json.dumps(data))


def classify(record):
    global analyzer
    tweet = record
    tkns = remove_disturbance(word_tokenize(tweet))
    sentiment = analyzer.classify(dict([tkn, True] for tkn in tkns))
    esDoc = {
        "tweet": tweet,
        "sentiment": sentiment
    }
    return esDoc
    # return str(tweet + " " + sentiment)


if __name__ == "__main__":
    consumerOfKafka = KafkaConsumer(bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumerOfKafka.subscribe(['test-topic'])
    analyzer = fetchSentimentAnalyzer()
    count=0
    elasticsearch = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    for message in consumerOfKafka:
        dataAfterClassifying = classify(message.value)
        elasticsearch.index(index='tweet', doc_type='default',id=count, body=dataAfterClassifying)
        count +=1



