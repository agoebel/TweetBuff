import os
from multiprocessing import Process, Pipe
import time
from pymongo import Connection
import tweepy

class TwitterConnector:
    def __init__(self):
        auth = tweepy.OAuthHandler(os.environ['TWITTER_CONSUMER_KEY'], os.environ['TWITTER_CONSUMER_SECRET'])
        auth.set_access_token(os.environ['TWITTER_ACCESS_TOKEN'], os.environ['TWITTER_ACCESS_TOKEN_SECRET'])
        self.api = tweepy.API(auth)
    
    def postTweet(self, tweet):
        self.api.update_status(tweet)

class MessageReceiver:
    def __init__(self):
        connection = Connection('localhost', 27017)
        db = connection['tweepbuff']
        self.tweets = db['tweets']
        self.twitterConnector = TwitterConnector()
            
    def processor(self, conn):
        while True:
            time.sleep(60)
            if conn.poll(0):
                output = conn.recv()
                self.tweets.insert(output)
                self.twitterConnector.postTweet(output['text'])
                
    def startup(self):
        parent_conn, child_conn = Pipe()
        p = Process(target=self.processor, args=(child_conn,))
        p.start()
        while True:
            data = { 'text' : raw_input() }
            if data['text'] == 'exit()':
                print("Terminating Process")
                p.terminate()
                break
            else:
                parent_conn.send(data)

if __name__ == '__main__':
    receiver = MessageReceiver()
    receiver.startup()