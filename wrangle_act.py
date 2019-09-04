#!/usr/bin/env python
# coding: utf-8

# In[92]:


# imports
import time
import pandas as pd
import numpy as np
import matplotlib as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import requests
from bs4 import BeautifulSoup

# Tweepy - Twitter API
import tweepy as tw

consumer_key = '3d36GklfJ1Vud6aG2CxZ4W5v0'
consumer_secret = 'QwPZ3L1Ef2ozwMggSrjDMPon6GgREIwSUOX1bXvmZFxK4aIvNq'
access_token = '1168946708274978816-nuiPdj6x6GziLDDujC1G3sjZOplGTJ'
access_secret = 'dbAWyRYCk0FplAfjkmqKqnw8GHdfckPB5V5yEvE2LKLtL'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

import json


# In[93]:


# variables


# ### Gathering Data
# #### Read data (3 sources: twitter file, image predictions, twitter api extra data)

# In[94]:


# Read data (3 sources: twitter file, image predictions, twitter api extra data)

# Read data - twitter file (csv)
df_twitter_file = pd.read_csv("twitter-archive-enhanced.csv")
df_twitter_file.head(3)

# List of twitter IDs
list_tweet_id=df_twitter_file.tweet_id
# list_tweet_id


# In[95]:


# Read data - image predictions from udacity's url: 
# https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv

url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)
df_image_pred = pd.read_csv(url,sep='\t')
# soup = BeautifulSoup(page_image_pred.content, 'html.parser')
df_image_pred.head(5)


# In[96]:


# Read data - Tweet JSON
# Store in txt file and read file into a DataFrame
# https://stackabuse.com/reading-and-writing-json-to-a-file-in-python/

#Tracking how long this block takes to run
start_time = time.time()

#Extracting specific data from the API
# full_text = tweet.full_text
# favorite_count = tweet.favorite_count
# retweet_count = tweet.retweet_count

    
# recreate the file each time the code is run
with open('tweet_json.txt', 'w', encoding='utf-8') as file:
    # Loop through the list of tweet ids from the csv file, pull the twitter data via API, convert to JSON and write/append to file
    count_deleted=0
    for tid in list_tweet_id:
        try:
            tweet = api.get_status(tid,tweet_mode='extended')
            tweet_json = json.dumps(tweet._json)
            file.write("\n" + tweet_json)
        except:
            count_deleted +=1
#             print(tid, "does not exist on Twitter, moving on to next Tweet ID from csv file...")

print("\n" + str(count_deleted) + " tweet_id(s) from the CSV file could not be found on twitter")


# Read the text file into a Data Frame keeping only certain columns

df_list=[] #create a list to hold data as its read from the file

with open("tweet_json.txt",'r', encoding='utf-8') as file:
    line_number=0
    while line_number <= sum(1 for line in open("tweet_json.txt",'r',encoding='utf-8')):
        line_number+=1
        line = file.readline()
        # Get tweet_id but check it's there first
        if line.find('"id":') != -1:
            tweet_id = line[line.find('"id":')+len('"id": ')  : line.find(",", line.find('"id":'))]
        else:
            print("'id' not found")
        # Get retweet_count but check it's there first
        if line.find('"retweet_count":') != -1:
            retweet_count = line[line.find('"retweet_count":')+len('"retweet_count": ') : line.find(",", line.find('"retweet_count":'))]
        else:
            print("'retweet_count not found")
        # Get favorite_count but check it's there first
        if line.find('"favorite_count":') != -1:
            favorite_count = line[line.find('"favorite_count":')+len('"favorite_count": ') : line.find(",", line.find('"favorite_count":'))]
        else:
            print("'favorite_count not found")

        # Append each entry from the JSON file to the list
        df_list.append({'tweet_id':tweet_id,
                        'retweet_count':retweet_count,
                        'favorite_count':favorite_count})

# Create data from from JSON file with specified columns
df_json = pd.DataFrame(df_list, columns = ['tweet_id','retweet_count','favorite_count'])


print("\n number of entries in data frame, including heading:", len(df_json))
print("\n number of entries in the file", sum(1 for line in open("tweet_json.txt",'r',encoding='utf-8')))
print()
print("\n This block of code took ", (time.time()-start_time)/60)


# ### Assess Data

# ##### Summary of findings based on code below
# 
# ##### Twitter flat file
# - After eyeballing the data, Col: name has dirty data e.g. "a" , "an", "the", etc. Asses and clean up
# - could combine the dog stages into one column with a label but must ensure a row does not have two dog stages
#      - Dog stages - looks like some dogs are in two stages - worth exploring to see if this is an error
#        On expecting the source data i.e. the tweets, it doesn't look to be - people just quote two dog stages and it get's recorded. Business will need to decide how to handle this
#        
# - after running the below code, some expanded urls:
#     - not every entry has an url - is this possible? 
#     - have two urls - this duplication is caused when a person uploads more than one photo in their tweet or 
#     - they include a url to another site in their tweet. 
#         - can clean this up by:
#             - removing duplicates 
#             - removing non twitter urls
#         Business must decide
# 
# ##### Image prediction file
# 
#  

# In[46]:


# Assess the twitter file

df_twitter_file.info(null_counts=True)


# In[45]:


df_twitter_file.describe()


# In[198]:


df_twitter_file.rating_denominator.value_counts()
labels = np.full(len(df_twitter_file.rating_denominator.value_counts()),"",dtype=object)
labels[0]='10'
df_twitter_file.rating_denominator.value_counts().plot(kind='pie',labels=labels)


# - By my understading: The denominators should all be ten
#     - Solution: rebase all denominator values to 10? 

# In[169]:


df_twitter_file.rating_numerator.value_counts()


# - a few extreme values but majority of people keep a number below 20

# In[176]:


print("Checking if tweet_id is unique. Expecting 2,356 values, counting: ",df_twitter_file.tweet_id.value_counts().size)


# In[173]:


print(df_twitter_file.doggo.value_counts())
print(df_twitter_file.floofer.value_counts())
print(df_twitter_file.pupper.value_counts())
print(df_twitter_file.puppo.value_counts())


# In[105]:


assess_dog_stages = df_twitter_file[['doggo','floofer','pupper','puppo']].drop_duplicates()

assess_dog_stages

# Example of a tweet with a dog in two stages or two or more dogs in the picture at different stages
# Getting URL to understand the data better and to determine if this is untidy data
# Conclusion: not untidy data however, some rows have duplicate URLs
pd.set_option('display.max_colwidth',-1)
df_twitter_file.expanded_urls[df_twitter_file['doggo']=='doggo'][df_twitter_file['pupper']=='pupper']


# In[162]:


# Checking col: expanded_urls 
# trying to understand why some rows have multiple urls that are the same
    # reason1: when a tweet has more than one photo, the url is captured as many times as there are photos
    # solution1: remove all duplicate urls, or change the urls incrementing the photo number by 1 for each
    # reason2: people include urls in their tweets
    # solution2: remove non twitter urls? 
    
# Copy dataframe - adding new columns
df_urls=df_twitter_file.copy()

df_urls['comma_count']=df_urls.expanded_urls.str.count(',')
df_urls['non_twit_acc']=df_urls.expanded_urls.str.startswith('https://twitter.com')

print("number of rows with non twitter urls: ", df_urls[df_urls['non_twit_acc']==False].shape)
print("number of rows with more than 1 url: ", df_urls[df_urls['comma_count'] >=1].shape)
print("number of rows with more than 1 twitter urls: ",df_urls[df_urls['comma_count'] >=1][df_urls['non_twit_acc'] == True].shape)


# #### Clean data
# 
# ##### Twitter file
# - col: name
# - col: expanded_urls
# 

# In[ ]:




