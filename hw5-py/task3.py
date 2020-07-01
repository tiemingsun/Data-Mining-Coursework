import time
import sys
import tweepy
import csv
import json
from collections import Counter
import random



class MyStreamListener(tweepy.StreamListener):
    
    def __init__(self):
        tweepy.StreamListener.__init__(self)
        self._hashtag_list = []
        self._writer_path = OUTPUT_PATH
        self._tweet_count = 0
        self._hashtag_count = 0
        
        
    def on_connect(self):

        print("Stream is connected!")
        
        # rewrite OUTPUT file
        fp = open(self._writer_path, 'w')
        fp.close()
        
        return
    

    def on_data(self, raw_data):
        data = json.loads(raw_data)
        tags = data["entities"]["hashtags"]
        
        if len(tags):
            self._tweet_count += 1
            print("I got new tweet with hashtags:", self._tweet_count)
            for single_tag in tags:
                
                self._hashtag_count += 1
                if len(self._hashtag_list) < 100:
                    self._hashtag_list.append(single_tag["text"])
                else:
                    # randomly choose a word in self._hashtag_list and replace it with prob=100/n
                    if random.random() < 100/self._hashtag_count:
                        rand_index = random.randint(0,99)
                        self._hashtag_list[rand_index] = single_tag["text"]
            
            # count top 3 hashtags so far
            hashtag_list_counter = Counter(self._hashtag_list)
            
            third_freq = -1
            distinct_freq = list(set(hashtag_list_counter.values()))
                                 
            if len(distinct_freq) > 3: 
                third_freq = sorted((distinct_freq), reverse=True)[2]
                
            top_hashtag_list = sorted(hashtag_list_counter.items(), key=lambda x:[-x[1], x[0]])
            
            # top_hashtag_list need to be truncuated
            if third_freq != -1:
                
                for i in range(len(top_hashtag_list)):
                    if top_hashtag_list[i][1] < third_freq:
                        top_hashtag_list = top_hashtag_list[:i]
                        break
            top_hashtag_list
            
            with open(self._writer_path, 'a') as fp:
                fp.write("The number of tweets with tags from the beginning: " + str(self._tweet_count) + '\n')
                for hash_tag, freq in top_hashtag_list:
                    fp.write(hash_tag + " : " + str(freq) + "\n")
                    
                fp.write("\n")
        return   




if __name__ == "__main__":

    OUTPUT_PATH = "///Users/tieming/inf553/hw5-py/task3.csv"
    # OUTPUT_PATH = sys.argv[2]

    consumer_key = "*******************************"
    consumer_secret = "*******************************"
    access_token = "*******************************"
    access_token_secret = "*******************************"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

    myStream.filter(track=['album'],languages=['en'])