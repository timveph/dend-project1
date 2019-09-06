#!/usr/bin/env python
# coding: utf-8

# In[1]:


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

# In[34]:


# Read data - twitter file (csv)
df_twitter_file = pd.read_csv("twitter-archive-enhanced.csv")
pd.set_option('display.max_colwidth',-1)
# List of twitter IDs
list_tweet_id=df_twitter_file.tweet_id


df_twitter_file.sample(3)
# list_tweet_id


# In[40]:


# Read data - image predictions from udacity's url: 
# https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv

url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)
df_image_pred = pd.read_csv(url,sep='\t')
# soup = BeautifulSoup(page_image_pred.content, 'html.parser')

print("Number of records in image prediction file:",df_image_pred.tweet_id.count())


# In[119]:


df_image_pred.sample(5)


# In[ ]:


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
            file.write(tweet_json + "\n")
        except:
            count_deleted +=1
#             print(tid, "does not exist on Twitter, moving on to next Tweet ID from csv file...")

print("\n" + str(count_deleted) + " tweet_id(s) from the CSV file could not be found on twitter")
print()
print("\n This block of code took ", (time.time()-start_time)/60)


# In[69]:


# Read the text file into a Data Frame keeping only certain columns
#Tracking how long this block takes to run
start_time = time.time()

df_list=[] #create a list to hold data as its read from the file

with open("tweet_json.txt",'r', encoding='utf-8') as file:
    line_number=0
    while line_number <= sum(1 for line in open("tweet_json.txt",'r',encoding='utf-8')):
        line_number+=1
        tweet_id=''
        retweet_count=''
        favorite_count=''
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
        if tweet_id != '':
            df_list.append({'tweet_id':tweet_id,
                            'retweet_count':retweet_count,
                            'favorite_count':favorite_count})
        else:
            print("not updating DataFrame due to tweet_id not being found")

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
# - (1) After eyeballing the data, Col: name has dirty data e.g. "a" , "an", "the", etc. Some names are set to "None"
#     - col: name - set None to null?
# - (A) combine the dog stages into one column with a label (doggo, floofer etc. into one column but must ensure a row does not have two dog stages
#      - (8) very few dog stages have been identified, might be problem with the file - can write code to check if we can get more dog stages from col: text
#      - Dog stages - looks like some dogs are in two stages - worth exploring to see if this is an error
#        On inspecting the source data i.e. the tweets, it doesn't look to be - people just quote two dog stages (i.e. they share a picture of their two dogs) and it get's recorded. 
#        
# - after running the below code, some expanded urls:
#     - (2) not every entry has an url - when a photo is not uploaded - 59 records identified without a url
#       - should not have a dog type or an entry in the image_pred file
#     - (B) have more than one urls - this duplication is caused when a person uploads more than one photo in their tweet (Twitter allows up to 4 photos) or 
#     - (3) they include a url to another site in their tweet. 
#         - can clean this up by:
#             - removing duplicates 
#             - (3) removing non twitter urls or put in own column
# 
# - (4) col: Source - contains a full html tag
# 
# - (C) col: retweet_status_id - Contains ReTweets ids - indicates a retweet (RT - columns retweeted_status_id.notnull())
# 
# - (5) col: rating_Denominator - some denominators are not 10
# - (6) col: rating_numerator - some numerators are extreme
#     - some information for both columns are captured incorrectly. For example, 786709082849828864 has a 9.75/10 rating. Another example is 810984652412424192 where the text includes "smiles 24/7" but was captured as a rating - no other rating info present. 
# 
# - (D) Col retweet* cols should be in their own data frame
# - (D) Col dog type and rating info in their own data frame
# 
# ##### Image prediction file
# - (7) col: jpg_url contains duplicate images - most likely belonging to retweets. Identify retweets from the main Twitter file
# - (9) remove records where a dog has not been identified
# 
# ##### json file
# - (E) col: retweet_count and favorite_count are string objects instead of integer

# In[61]:


# Assess the 3 files

print("downloaded twitter file")
df_twitter_file.info(null_counts=True)
print()
print("image prediction file")
df_image_pred.info(null_counts=True)
print()
print("Twitter API data extraction file")
df_json.info(null_counts=True)


# In[52]:


print("Number of missing expanded_URLS from the twitter file is:", df_twitter_file[df_twitter_file.expanded_urls.isnull()].tweet_id.count())
print("Number of tweets in the Twitter downloaded file that contains RTs:",df_twitter_file[df_twitter_file.retweeted_status_id.notnull()].tweet_id.count())
# df_twitter_file.sample(5)
# print()
# print(df_image_pred.sample(3))
# print()
# print(df_json.sample(3))
# df_twitter_file[df_twitter_file.tweet_id == 783695101801398276]
# df_json[df_json.tweet_id== 886267009285017600]
# df_twitter_file[df_twitter_file.expanded_urls.isnull()]


# In[104]:


df_twitter_file.describe()


# In[8]:


df_image_pred.describe()


# In[56]:


df_json.describe()


# In[10]:


df_twitter_file[df_twitter_file.tweet_id.duplicated()]
#no duplicated ids


# In[57]:


df_image_pred[df_image_pred.tweet_id.duplicated()]
#no duplicated ids
# df_image_pred[df_image_pred.jpg_url == 'https://pbs.twimg.com/media/CWza7kpWcAAdYLc.jpg']
# df_twitter_file[df_twitter_file.tweet_id == 679158373988876288]
# df_twitter_file[df_twitter_file.tweet_id == 754874841593970688]


# In[71]:


df_json[df_json.tweet_id.duplicated()]
#no duplicate ids


# In[115]:


df_twitter_file[df_twitter_file.in_reply_to_status_id.notnull()].sample(3)
#tweets that seem to have a reply have a bad rating or multiple, have no expanded_urls ALTHOUGH
# SOME are valid with photos and without photos


# In[117]:


# df_twitter_file.rating_denominator.value_counts()
# print(df_twitter_file[df_twitter_file.rating_denominator !=10].sample(3))/

print("Rating_Denominator values excluding 10")
df_twitter_file[df_twitter_file['rating_denominator'] != 10].groupby('rating_denominator')['rating_denominator'].count()


# - By my understading: The denominators should all be ten
#     - Solution: rebase all denominator values to 10? 

# In[169]:


df_twitter_file.rating_numerator.value_counts()


# In[105]:


df_twitter_file[df_twitter_file.rating_numerator >= 20].sample(3)


# - a few extreme values but majority of people keep a number below 20

# In[176]:


print("Checking if tweet_id is unique. Expecting 2,356 values, counting: ",df_twitter_file.tweet_id.value_counts().size)


# In[55]:


print("\n doggo")
print(df_twitter_file.doggo.value_counts())
print("\n floofer")
print(df_twitter_file.floofer.value_counts())
print("\n pupper")
print(df_twitter_file.pupper.value_counts())
print("\n puppo")
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


# ### Clean data

# ##### Twitter file
# - col: identify ReTweets and remove from file
# - col: name - remove non-names
# - col: expanded_urls: split out non-twitter urls into its own column
# - col: expanded_urls: a tweet with more one url indicates a tweet with more than one photo, identify the number of photos and create a column to capture the number of photos included in each tweet. In the expanded_urls column, only include a link to the first photo or tweet itself. 
# - col: expanded_urls: for missing urls, create url based on tweet_id and basic structure of a tweet e.g. "https://twitter.com/dog_rates/status/"
# - col: identify ReTweets and remove from file
# - Update dog status
# - Correct faulty raitings
# 
# ##### Image Pred 
# - Remove retweets (use (retweet) tweet_id list from twitter file)
# - Restructure file
# - Exclude records where no picture has been identified as a dog
# 
# ##### twitter_json
# - Remove retweets (use (retweet) tweet_id list from twitter file)
# 
# 

# In[ ]:




