import tweepy
import json
import pymysql
import apiConfig
import dbConfig
import ibmConfig
import time
import pytz
import datetime
from ibm_watson import NaturalLanguageUnderstandingV1
from ibm_watson.natural_language_understanding_v1 import Features, SentimentOptions


host=dbConfig.host
port=dbConfig.port
dbname=dbConfig.dbname
user=dbConfig.user
password=dbConfig.password

conn = pymysql.connect(host, user=user,port=port,
                           passwd=password, db=dbname)
c = conn.cursor()
print('Conectado ao Banco de Dados')

global api
# API keys are stored in a separate file
access_token = apiConfig.access_token
access_token_secret = apiConfig.access_token_secret
consumer_key = apiConfig.consumer_key
consumer_secret = apiConfig.consumer_secret

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
print('Conectado a API')

def sentiment_analysis(text):
    print('Realizando Análise de Sentimentos')
    # initialize IBM NLU client
    natural_language_understanding = NaturalLanguageUnderstandingV1(
        version='2018-11-16',
        iam_apikey=ibmConfig.iam_apikey,
        url='https://gateway.watsonplatform.net/natural-language-understanding/api'
    )
    # send text to IBM Cloud to fetch analysis result
    response = natural_language_understanding.analyze(text=text, features=Features(sentiment=SentimentOptions())).get_result()
    return response

#def time_conversion(tweet):
#    date = datetime.datetime.strptime(tweet, '%a %b %d %H:%M:%S +0000 %Y')
#    date = date.astimezone(pytz.timezone('US/Mountain'))
#    return date.strftime('%Y-%m-%d %H:%M:%S')
#Identify tweet subject
def bank_id(tweet):
    if tweet.find('Banco do Brasil') != -1 or tweet.find('banco do Brasil') != -1 or tweet.find('@BancodoBrasil') != -1 or tweet.find('banco do brasil') != -1:
        return '1'
    if tweet.find('Bradesco') != -1 or tweet.find('bradesco') != -1 or tweet.find('@Bradesco') != -1:
        return '237'
    if tweet.find('Santander') != -1 or tweet.find('santander') != -1 or tweet.find('@santander_br') != -1:
        return '33'
    if tweet.find('Nubank') != -1 or tweet.find('nubank') != -1 or tweet.find('@nubank') != -1:
        return '260'
    if tweet.find('banco inter') != -1 or tweet.find('@bancointer') != -1 or tweet.find('@BANCOINTER') != -1 or tweet.find('@Bancointer') != -1 or tweet.find('Banco Inter') != -1 or tweet.find('Inter') != -1:
        return '77'
    if tweet.find('Ita') != -1 or tweet.find('itau') != -1 or tweet.find('@itau') != -1:
        return '341'
    if tweet.find('Caixa Econômica') != -1 or tweet.find('caixa econômica') != -1 or tweet.find('Caixa Economica') != -1 or tweet.find('@Caixa') != -1 or tweet.find('caixa') != -1 or tweet.find('Caixa') != -1:
        return '104'
    else:
        return '?'


# Class for defining a Tweet
class Tweet():

    # Data on the tweet
    def __init__(self, id, text, user, followers, date, location, bank, sentiment, score):
        self.id = id
        self.text = text
        self.user = user
        self.followers = followers
        self.date = date
        self.location = location
        self.bank = bank
        self.sentiment = sentiment
        self.score = score

    # Inserting that data into the DB
    def insertTweet(self):
        query = "INSERT INTO tweets (id, tweetText, user, followers, date, location, bankId, sentiment, score) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        c.execute(query, (self.id, self.text, self.user, self.followers, self.date, self.location, self.bank, self.sentiment, self.score))
        conn.commit()

# Stream Listener class
class TweetStreamListener(tweepy.StreamListener):

    # When data is received
    def on_data(self, data):

        # Error handling because teachers say to do this
        try:

            # Make it JSON
            tweet = json.loads(data)
            bk = json.dumps(tweet)
            bank = bank_id(bk)
            print(bank)
            a = '*'
            print(a * 50)
            print(tweet)
            print(a * 50)

            #date = time_conversion(tweet['created_at'])
            # filter out retweets
            if not tweet['retweeted'] and 'RT @' not in tweet['text'] and '?' not in bank:

                if len(tweet['text']) < 140:
                    sentiment = sentiment_analysis(tweet['text'])
                    # Get user via Tweepy so we can get their number of followers
                    user_profile = api.get_user(tweet['user']['screen_name'])
                # assign all data to Tweet object
                    tweet_data = Tweet(
                        tweet['id'],
                        tweet['text'], # Have to use this encode because sometimes people tweet weird characters
                        tweet['user']['screen_name'],
                        user_profile.followers_count,
                        tweet['created_at'],
                        tweet['user']['location'],
                        bank,
                        sentiment['sentiment']['document']['label'],
                        sentiment['sentiment']['document']['score'])
                    # Insert that data into the DB
                    tweet_data.insertTweet()
                    print("Successo < 140!")


                if 'retweeted_status' not in tweet.keys():
                    sentiment = sentiment_analysis(tweet['extended_tweet']['full_text'])
                    # Get user via Tweepy so we can get their number of followers
                    user_profile = api.get_user(tweet['user']['screen_name'])
                # assign all data to Tweet object
                    tweet_data = Tweet(
                        tweet['id'],
                        tweet['extended_tweet']['full_text'], # Have to use this encode because sometimes people tweet weird characters
                        tweet['user']['screen_name'],
                        user_profile.followers_count,
                        tweet['created_at'],
                        tweet['user']['location'],
                        bank,
                        sentiment['sentiment']['document']['label'],
                        sentiment['sentiment']['document']['score'])
                # Insert that data into the DB
                    tweet_data.insertTweet()
                    print("Successo!")

                if 'retweeted_status' in tweet.keys():
                    sentiment = sentiment_analysis(tweet['retweeted_status']['extended_tweet']['full_text'])
                    # Get user via Tweepy so we can get their number of followers
                    user_profile = api.get_user(tweet['user']['screen_name'])
                    # assign all data to Tweet object
                    tweet_data = Tweet(
                        tweet['id'],
                        tweet['retweeted_status']['extended_tweet']['full_text'], # Have to use this encode because sometimes people tweet weird characters
                        tweet['user']['screen_name'],
                        user_profile.followers_count,
                        tweet['created_at'],
                        tweet['user']['location'],
                        bank,
                        sentiment['sentiment']['document']['label'],
                        sentiment['sentiment']['document']['score'])
                    # Insert that data into the DB
                    tweet_data.insertTweet()
                    print("Successo Tweet Extendido!")

        # Let me know if something bad happens
        except Exception as e:
            print(e)
            pass

        return True

# Driver
if __name__ == '__main__':

    # Run the stream!
    l = TweetStreamListener()
    stream = tweepy.Stream(auth, l, tweet_mode= 'extended')

    # Filter the stream for these keywords. Add whatever you want here!
    stream.filter(track=['Banco do Brasil', '@BancodoBrasil', 'Itaú', '@itau','itau', 'Bradesco', '@Bradesco', 'Caixa Econômica', '@Caixa','Caixa Economica', 'Santander','@santander_br', 'Nubank', '@nubank', 'Banco Inter', '@Bancointer'], languages=["pt"])
