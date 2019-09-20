#!/usr/bin/env python
# coding: utf-8

# In[1]:


# imports
from datetime import datetime
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
from wordcloud import WordCloud, STOPWORDS
import requests
from bs4 import BeautifulSoup

# Tweepy - Twitter API
import tweepy as tw

# consumer_key = '3d36G'
# consumer_secret = 'QwP'
# access_token = '1168'
# access_secret = 'db'

# auth = tw.OAuthHandler(consumer_key, consumer_secret)
# auth.set_access_token(access_token, access_secret)

# api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

import json


# In[2]:


# variables


# ### Gathering Data
# #### Read data (3 sources: twitter file, image predictions, twitter api extra data)

# In[3]:


# Read data - twitter file (csv)
df_twitter_file = pd.read_csv("twitter-archive-enhanced.csv")
pd.set_option('display.max_colwidth',-1)
# List of twitter IDs
list_tweet_id=df_twitter_file.tweet_id
# list of retweet ids
df_retweet_ids = df_twitter_file[df_twitter_file.retweeted_status_id.notnull()].tweet_id

# df_twitter_file.sample(3)
print("Number of retweets:",df_retweet_ids.shape)
df_retweet_ids.sample(3)


# In[4]:


# Read data - image predictions from udacity's url: 
# https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv

url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)
df_image_pred = pd.read_csv(url,sep='\t')
# soup = BeautifulSoup(page_image_pred.content, 'html.parser')

print("Number of records in image prediction file:",df_image_pred.tweet_id.count())


# In[5]:


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


# In[6]:


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

# #### Quality issues
# ##### Twitter flat file
# - After eyeballing the data, Col: name has dirty data e.g. "a" , "an", "the", etc. Some names are set to "None"
#     - col: name - set None to null?
# - very few dog stages have been identified, might be problem with the file - can write code to check if we can get more dog stages from col: text
#     - Dog stages - looks like some dogs are in two stages - worth exploring to see if this is an error
#        On inspecting the source data i.e. the tweets, it doesn't look to be - people just quote two dog stages (i.e. they share a picture of their two dogs) and it get's recorded. 
# - Col: expanded_urls
#     - not every entry has an url - when a photo is not uploaded - 59 (3) records identified without a url - this could be a reply or retweet without a picture
#     - have more than one urls - this duplication is caused when a person uploads more than one photo in their tweet (Twitter allows up to 4 photos) or 
#     - they include a url to another site in their tweet.
#     - some urls have videos only (you can only have 1 video per tweet)
# - Column source contains a full html tag
# - Column text contains html urls that are duplicates of the urls in the expanded url column
# - File contains retweets - remove from file
# - col: rating_Denominator - some denominators are not 10
# - col: rating_numerator - some numerators are extreme
#     - some information for both columns are captured incorrectly. For example, 786709082849828864 has a 9.75/10 rating. Another example is 810984652412424192 where the text includes "smiles 24/7" but was captured as a rating - no other rating info present. 
# - col timestamp and retweet*timestamp is set to object
# - tweet_id is set to integer
# 
# ##### image prediction file
# - remove records where a dog has not been identified
# - col: jpg_url contains duplicate images - most likely belonging to retweets. Identify retweets from the main Twitter file
# - tweet_id is set to integer
# 
# ##### json file
# - col: retweet_count and favorite_count are string objects instead of integer
# - tweet_is is set to integer

# #### Tidy issues
# 
# - doggo, floofer, pupper, puppo columns in twitter_archive_enhanced.csv should be combined into a single column as this is one variable that identify stage of dog.
# - Information about one type of observational unit (tweets) is spread across three different files/dataframes. So these three dataframes should be merged as they are part of the same observational unit.

# In[8]:


# Assess the 3 files

print("downloaded twitter file")
df_twitter_file.info(null_counts=True)
print()
print("image prediction file")
df_image_pred.info(null_counts=True)
print()
print("Twitter API data extraction file")
df_json.info(null_counts=True)


# In[10]:


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


# In[11]:


df_twitter_file.describe()


# In[12]:


df_image_pred.describe()
df_image_pred.sample(3)
df_image_pred[df_image_pred.tweet_id == 862096992088072192]


# In[13]:


df_json.describe()


# In[14]:


df_twitter_file[df_twitter_file.tweet_id.duplicated()]
#no duplicated ids


# In[15]:


df_image_pred[df_image_pred.tweet_id.duplicated()]
#no duplicated ids
# df_image_pred[df_image_pred.jpg_url == 'https://pbs.twimg.com/media/CWza7kpWcAAdYLc.jpg']
# df_twitter_file[df_twitter_file.tweet_id == 679158373988876288]
# df_twitter_file[df_twitter_file.tweet_id == 754874841593970688]


# In[16]:


df_json[df_json.tweet_id.duplicated()]
#no duplicate ids


# In[17]:


df_twitter_file[df_twitter_file.in_reply_to_status_id.notnull()].sample(3)
#tweets that seem to have a reply have a bad rating or multiple, have no expanded_urls ALTHOUGH
# SOME are valid with photos and without photos


# In[18]:


# df_twitter_file: Identify bad names

# df_twitter_file.name.value_counts()
df_twitter_file[df_twitter_file['name'].str.contains('^[a-z]+')].name.unique()
# Bad names include: 'such', 'a', 'quite', 'not', 'one', 'incredibly', 'mad', 'an',
#       'very', 'just', 'my', 'his', 'actually', 'getting', 'this',
#       'unacceptable', 'all', 'old', 'infuriating', 'the', 'by',
#       'officially', 'life', 'light', 'space'
# change "None to NaN"


# In[19]:


# df_twitter_file.rating_denominator.value_counts()
# print(df_twitter_file[df_twitter_file.rating_denominator !=10].sample(3))/

print("Rating_Denominator values excluding 10")
df_twitter_file[df_twitter_file['rating_denominator'] != 10].groupby('rating_denominator')['rating_denominator'].count()


# - By my understading: The denominators should all be ten
#     - Solution: rebase all denominator values to 10? 

# In[20]:


df_twitter_file.rating_numerator.value_counts()


# In[21]:


df_twitter_file[df_twitter_file.rating_numerator >= 20].sample(3)


# - a few extreme values but majority of people keep a number below 20

# In[22]:


print("Checking if tweet_id is unique. Expecting 2,356 values, counting: ",df_twitter_file.tweet_id.value_counts().size)


# In[23]:


print("\n doggo")
print(df_twitter_file.doggo.value_counts())
print("\n floofer")
print(df_twitter_file.floofer.value_counts())
print("\n pupper")
print(df_twitter_file.pupper.value_counts())
print("\n puppo")
print(df_twitter_file.puppo.value_counts())


# In[24]:


assess_dog_stages = df_twitter_file[['doggo','floofer','pupper','puppo']].drop_duplicates()

assess_dog_stages

# Example of a tweet with a dog in two stages or two or more dogs in the picture at different stages
# Getting URL to understand the data better and to determine if this is untidy data
# Conclusion: not untidy data however, some rows have duplicate URLs
pd.set_option('display.max_colwidth',-1)
df_twitter_file.expanded_urls[df_twitter_file['doggo']=='doggo'][df_twitter_file['pupper']=='pupper']


# In[25]:


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
# - col: identify ReTweets and replies and remove from file
# - col: name - remove non-names (they start with lowercase letters)
# - col: expanded_urls: split out non-twitter urls into its own column
# - col: expanded_urls: a tweet with more one url indicates a tweet with more than one photo, identify the number of photos and create a column to capture the number of photos included in each tweet. In the expanded_urls column, only include a link to the tweet itself. 
# - col: expanded_urls: for missing urls, create url based on tweet_id and basic structure of a tweet e.g. "https://twitter.com/dog_rates/status/"
# - Update dog stage
# - Correct faulty raitings
# - Change col timestamp and datetime variable
# - Remove HTML tags from the source column
# - drop retweet and reply columns
# - Remove HTLM from text column

# In[106]:


#Copy the DataFrames - keeping the orginal data for comparison to the clean data later
dfc_twitter_file = df_twitter_file.copy()
print("\ndfc_twitter_file - Total number of records before cleaning:",dfc_twitter_file.tweet_id.value_counts().size)
dfc_image_pred = df_image_pred.copy()
print("\ndfc_image_pred - Total number of records before cleaning:",dfc_image_pred.tweet_id.value_counts().size)
dfc_json = df_json.copy()
print("\ndfc_json - Total number of records before cleaning:",dfc_json.tweet_id.value_counts().size)


# #### Clean
# ##### Remove retweets and replies (such records are out of scope for this project)

# In[107]:


# code to remove retweets and replies
# dfc_twitter_file: Remove tweets that are replies
dfc_twitter_file = dfc_twitter_file[dfc_twitter_file.in_reply_to_status_id.isnull()]
# dfc_twitter_file: Remove tweets that are retweets
dfc_twitter_file = dfc_twitter_file[dfc_twitter_file.retweeted_status_id.isnull()]

# Test
# dfc_twitter_file: test removal of replies and retweets
print("dfc_twitter_file: Are there any tweets that are replies?")
print(dfc_twitter_file[dfc_twitter_file.in_reply_to_status_id.notnull()])
print("\ndfc_twitter_file: Are there any tweets that are retweets?")
print(dfc_twitter_file[dfc_twitter_file.retweeted_status_id.notnull()])


# A statement to show how many records are left after removing replies and retweets from the file
print("\ndfc_twitter_file: Total number of records after removing replies and retweets: ", dfc_twitter_file.tweet_id.value_counts().size)


# In[108]:


# Now that the records have been removed, we now need to remove the columns
# dfc_twitter_file: drop replies and retweet columns
# in_reply_to_status_id
# in_reply_to_user_id
# retweeted_status_id 
# retweeted_status_user_id
# retweeted_status_timestamp

print("dfc_twitter_file: The number of columns in the file before they are dropped:",dfc_twitter_file.shape[1])

# Drop columns based on position in file
dfc_twitter_file.drop(dfc_twitter_file.columns[[1,2,6,7,8]], axis = 1, inplace=True)
             
print("dfc_twitter_file: The number of columns in the file after they are dropped:",dfc_twitter_file.shape[1])
print("\ndfc_twitter_file: Total number of records after adding missing urls: ", dfc_twitter_file.tweet_id.value_counts().size)


# In[109]:


# Test - make sure visually that the columns have been removed
# dfc_twitter_file: Visual check for dropped columns
dfc_twitter_file.sample(3)


# #### Clean
# ##### Clean "bad names" - names were not captured correctly in the twitter file
# It was observed that the 'bad names' all start with lowercase

# In[110]:


# code to identify bad names

# dfc_twitter_file: Remove bad names
print("A list of names starting with lowercase: ",dfc_twitter_file[dfc_twitter_file['name'].str.contains('^[a-z]+')].name.unique())


# In[111]:


# dfc_twitter_file: Identify bad names and replace with None
print("dfc_twitter_file: The current total number of names set to 'None' (before fix):",dfc_twitter_file[dfc_twitter_file['name']=='None'].name.count())

# get a unique list of bad names - bad names start with lowercase
bad_names = dfc_twitter_file[dfc_twitter_file['name'].str.contains('^[a-z]+')].name.unique()

# check the number of bad names
print("\ndfc_twitter_file: A list and count of bad names being replaced by 'None'\n")
for name in bad_names:
    print(name,"(",dfc_twitter_file.loc[dfc_twitter_file['name']==name].name.count(),")")
    dfc_twitter_file.name = dfc_twitter_file.name.replace(name,"None")


# In[112]:


# test
print("dfc_twitter_file: Total number of names set to 'None':",dfc_twitter_file[dfc_twitter_file['name']=='None'].name.count())

print("\ndfc_twitter_file: Total number of records after adding missing urls: ", dfc_twitter_file.tweet_id.value_counts().size)


# #### Clean
# ##### Clean expanded_urls
# - not every entry has an url - when a photo is not uploaded - 59 (3) records identified without a url - this could be a reply or retweet without a picture
# - have more than one urls - this duplication is caused when a person uploads more than one photo in their tweet (Twitter allows up to 4 photos) or
# - they include a url to another site in their tweet.
# - some urls have videos only (you can only have 1 video per tweet)

# In[113]:


# Code
# DO NOT DELETE OR CHANGE EXPANDED_URLS in the first instance
# KEEP THE FIRST URL IN THE LIST
# PUT NON-TWITTER URLS IN ANOTHER COLUMN
# RECREATE ALL URLS TO POINT TO TWITTER USING THE TWEET ID IN A NEW COLUMN CALLED: TWITTER URL

# dfc_twitter_file.expanded_urls.str.slice(start=0, stop=len(dfc_twitter_file.expanded_urls.str.split(",",n=1)),step=1)
# dfc_twitter_file.expanded_urls.str.split(",",n=1).str[0].str.startswith("https://twitter.com/")

# Create a tempory column to hold urls
dfc_twitter_file['temp_url']=dfc_twitter_file.expanded_urls.str.split(",",n=1).str[0]
dfc_twitter_file.temp_url

# create a new column to hold non twitter urls
dfc_twitter_file['non_twitter_url']=dfc_twitter_file[~dfc_twitter_file.temp_url.str.startswith("https://twitter.com/", na=False)].temp_url

# create a column to hold twitter urls
dfc_twitter_file['twitter_url']=dfc_twitter_file[dfc_twitter_file.temp_url.str.startswith("https://twitter.com/", na=False)].temp_url
# remove video/1 or photo/n from url leaving url in format https://twitter.com/dog_rates/status/tweet_id
dfc_twitter_file.twitter_url = dfc_twitter_file.twitter_url.str.slice(start=0, stop=len('https://twitter.com/dog_rates/status/')+18, step=1)

# drop temporary column
dfc_twitter_file.drop('temp_url',axis = 1, inplace=True)

# dfc_twitter_file.info()

# test new columns have been created
dfc_twitter_file.sample(3)


# In[114]:


# code
# dfc_twitter_file: Fill in missing URLs using the tweet_id
print("dfc_twitter_file: The number of missing urls before fix: ", dfc_twitter_file.tweet_id.value_counts().size - dfc_twitter_file[dfc_twitter_file.twitter_url.notnull()].twitter_url.count())
dfc_twitter_file.tweet_id.value_counts().size

dfc_twitter_file["twitter_url"].fillna("https://twitter.com/dog_rates/status/"+dfc_twitter_file.tweet_id.apply(str), inplace = True)
print(dfc_twitter_file[dfc_twitter_file.twitter_url.isnull()])

# test - check for missing urls
print("dfc_twitter_file: The number of missing urls after fix: ", dfc_twitter_file.tweet_id.value_counts().size - dfc_twitter_file[dfc_twitter_file.twitter_url.notnull()].twitter_url.count())
print("\ndfc_twitter_file: Total number of records after adding missing urls: ", dfc_twitter_file.tweet_id.value_counts().size)


# #### Clean
# ##### Create a new column that identifies how many media files were included in each tweet
# 

# In[115]:


# code
# Count the number of Twitter URL's and store the number in a new column called photo_per_tweet

print("The number of tweets with n media files:\n",dfc_twitter_file.expanded_urls.str.count("https://twitter.com/").value_counts())
dfc_twitter_file['photo_per_tweet'] = dfc_twitter_file.expanded_urls.str.count("https://twitter.com/")
print("\nThe number of tweets with n media files in column photo_per_tweet:\n",dfc_twitter_file.photo_per_tweet.value_counts())


# dfc_twitter_file[dfc_twitter_file.tweet_id == 706153300320784384]


# #### Tidy
# ##### doggo, floofer, pupper, puppo columns in twitter_archive_enhanced.csv should be combined into a single column as this is one variable that identify stage of dog.
# Please be aware that some tweets have multiple stages

# In[116]:


# code 
# dfc_twitter_file: create a column for dog status (category type)
# search the text for a list of dog stages
# dfc_twitter_file.sample(15)


df = dfc_twitter_file.copy()

search = ['doggo', 'floofer', 'pupper', 'puppo']
# search the text for the 4 dog stages and add them to a list. Convert this list to a string and store in column dog_stage
dfc_twitter_file['dog_stage'] = dfc_twitter_file['text'].str.findall('|'.join(search)).apply(' '.join)
dfc_twitter_file['dog_stage'] = dfc_twitter_file['dog_stage'].apply(lambda x: ' '.join(sorted(x.split())))
  
dfc_twitter_file.dog_stage = dfc_twitter_file.dog_stage.replace('pupper pupper pupper', 'pupper')
dfc_twitter_file.dog_stage = dfc_twitter_file.dog_stage.replace('doggo doggo pupper', 'doggo pupper')
dfc_twitter_file.dog_stage = dfc_twitter_file.dog_stage.replace('pupper pupper', 'pupper')

# df[df.dog_stage=='pupper, pupper, pupper']

# test 
dfc_twitter_file.copy().dog_stage.value_counts()
# df.sample(10)
# df.dog_stage.dtype
# df.info()


# #### Clean
# ##### clean numerator and denominator rating columns
# - col: rating_Denominator - some denominators are not 10
# - col: rating_numerator - some numerators are extreme
# - some information for both columns are captured incorrectly. For example, 786709082849828864 has a 9.75/10 rating. Another example is 810984652412424192 where the text includes "smiles 24/7" but was captured as a rating - no other rating info present.

# In[117]:


dfc_twitter_file[dfc_twitter_file.tweet_id == 883482846933004288]


# In[118]:


# dfc_twitter_file: fix the rating_denominator and numerator
# Focus on the odd denominators as a first
dfc_twitter_file.rating_denominator.value_counts()
# dfc_twitter_file[dfc_twitter_file.rating_denominator != 10]


# In[119]:


dfc_twitter_file.rating_numerator.value_counts()


# In[120]:


dfc_twitter_file.rating_denominator.value_counts()


# In[121]:


# dfc_twitter_file: fix the rating_denominator and numerator

# code to clean numerator and denominator

rating = dfc_twitter_file.text.str.extract('((?:\d+\.)?\d+)\/(\d+)', expand=True)
rating.columns = ['rating_numerator', 'rating_denominator']
dfc_twitter_file['rating_numerator'] = rating['rating_numerator'].astype(float)
dfc_twitter_file['rating_denominator'] = rating['rating_denominator'].astype(float)


# manual change ratings by reviewing the tweets
# dfc_twitter_file.loc[df.tweet_id == 740373189193256964, ['rating_numerator', 'rating_denominator']] = 14, 10
# dfc_twitter_file.loc[df.tweet_id == 722974582966214656, ['rating_numerator', 'rating_denominator']] = 13, 10
# dfc_twitter_file.loc[df.tweet_id == 722974582966214656, ['rating_numerator', 'rating_denominator']] = 13, 10
# dfc_twitter_file.loc[df.tweet_id == 716439118184652801, ['rating_numerator', 'rating_denominator']] = 11, 10
# dfc_twitter_file.loc[df.tweet_id == 682962037429899265, ['rating_numerator', 'rating_denominator']] = 10, 10
# dfc_twitter_file.loc[df.tweet_id == 666287406224695296, ['rating_numerator', 'rating_denominator']] = 9, 10
# set this one to 10/10 as there was no rating
# dfc_twitter_file.loc[df.tweet_id == 810984652412424192, ['rating_numerator', 'rating_denominator']] = 10, 10

# For the remaining records with a denominator not set to 10, programmatically fix - base denominator to 10 and rebase numerator
dfc_twitter_file['rating_numerator'] = dfc_twitter_file.loc[dfc_twitter_file['rating_denominator'] > 10, 'rating_numerator'] = dfc_twitter_file.rating_numerator / (dfc_twitter_file.rating_denominator / 10)
# changes denominator (do this after changing numerator)
dfc_twitter_file['rating_denominator'] = dfc_twitter_file.loc[dfc_twitter_file['rating_denominator'] > 10, 'rating_denominator'] = dfc_twitter_file.rating_denominator / (dfc_twitter_file.rating_denominator / 10)

# test denominator
dfc_twitter_file.rating_denominator.value_counts()


# In[122]:


# test numerator
dfc_twitter_file.rating_numerator.value_counts()


# In[123]:


# test - look for records that don't have a denominator of 10
# dfc_twitter_file[dfc_twitter_file.rating_denominator != 10]
dfc_twitter_file[dfc_twitter_file.rating_denominator != 10]


# In[ ]:





# #### Clean
# ##### Change data types
# - Change tweet_id to object
# - timestamp to datetime
# - photo_per_tweet to integer
# - rating_denominator to integer

# In[124]:


# dfc_twitter_file: change numeric objects (timestamp) to integers/date in the various tables 
print("Check to see the column data types before they are changed accordingly\n")
print(dfc_twitter_file.dtypes)


# In[125]:


#code 
# change tweet_id to object
dfc_twitter_file['tweet_id'] = dfc_twitter_file.tweet_id.astype(np.object)
# Change timestamp to datetime
dfc_twitter_file['timestamp'] = pd.to_datetime(dfc_twitter_file['timestamp'])
# change photo_per_tweet from float to int8 (save memory) and also fill any missing values as 0
dfc_twitter_file['photo_per_tweet'] = dfc_twitter_file['photo_per_tweet'].fillna(0).astype(np.int8)
# change rating columns 
# dfc_twitter_file.rating_numerator = dfc_twitter_file.rating_numerator.astype(np.int64)
dfc_twitter_file.rating_denominator = dfc_twitter_file.rating_denominator.astype(np.int64)

# dfc_twitter_file.sample(5)

#test 
# A print out of the dataframe info to see what has changed 
print("Check to see if the column data types have changed accordingly")
print(dfc_twitter_file.dtypes)


# In[126]:


# dfc_twitter_file.rating_numerator.value_counts()
# dfc_twitter_file[dfc_twitter_file.rating_numerator < 0]


# In[127]:


# dfc_twitter_file.rating_denominator.value_counts()


# #### Clean
# #####  remove html tags from "source" column
# 

# In[128]:


# code to assess what we are dealing with
# dfc_twitter_file
# Clean the source column removing HTML tags

print("List of unique values in source column before removing html tags:\n",dfc_twitter_file.source.unique())


# In[129]:


# code to remove html tags and keep just the text
# Use of Beautiful soup and lambda to remove html tags from source column
dfc_twitter_file['source'] = dfc_twitter_file.source.apply(lambda x: BeautifulSoup(x,'lxml').get_text())

#test
print("List of unique values in source column after removing html tags:\n",dfc_twitter_file.source.unique())
print()
dfc_twitter_file.sample(3)


# #### Clean
# ##### remove html urls (short urls) from "text" column

# In[130]:


# code to Remove HTLM urls from text column
# Info on t.co's way of shortening: https://help.twitter.com/en/using-twitter/how-to-tweet-a-link
# Each shortened link will be 23 characters long no matter if the original is longer or shorter
dfc_twitter_file['text'] = dfc_twitter_file['text'].replace(r'http\S+', '', regex=True).replace(r'www\S+', '', regex=True)

# test
dfc_twitter_file.sample(3)


# In[ ]:





# #### Clean
# #####  remove records without photos

# In[131]:


#dfc_twitter_file
# drop records with no photos
# drop the doggo, floofer, pupper, puppo, expanded_urls columns
# reorder the columns

print("The shape of the data before records with no photos are removed:",dfc_twitter_file.shape)


# In[132]:


# code 
# Count after removing records without photos

dfc_twitter_file = dfc_twitter_file[dfc_twitter_file.photo_per_tweet > 0]

# test
print("The shape of the data after records with no photos are removed:",dfc_twitter_file.shape)


# #### Clean
# #####  remove columns  no longer needed
# 

# In[133]:


# drop the 'doggo','floofer','pupper','puppo','expanded_urls'

print("The shape of the data before columns are removed:",dfc_twitter_file.shape)
dfc_twitter_file.dtypes


# In[134]:


# code 
# drop columns not needed

dfc_twitter_file.drop(columns=['doggo','floofer','pupper','puppo','expanded_urls'], inplace=True)

# test
print("The shape of the data after columns are removed:",dfc_twitter_file.shape)

dfc_twitter_file.info()


# In[135]:


#dfc_twitter_file: reorder the columns

# print("Column order before\n")
# dfc_twitter_file.info()


# In[136]:


# get a list of tweet_ids
s_tweet_ids = dfc_twitter_file.tweet_id
s_tweet_ids.shape


# ### Clean Data

# 
# ##### Image Pred 
# - Remove retweets (use (retweet) tweet_id list from twitter file)
# - Restructure file (p1, p2, etc.)
# - Exclude records where no picture has been identified as a dog
# - change tweet_id to object

# In[137]:


# image_pred: 

# dfc_image_pred.info()
# dfc_image_pred.describe()
print("p1 median:",dfc_image_pred.p1_conf.median())
print("p2 max:",dfc_image_pred.p2_conf.max())
print("p3 max:",dfc_image_pred.p3_conf.max())

dfc_image_pred.sample(5)
dfc_image_pred.shape


# In[138]:


dfc_image_pred[dfc_image_pred.tweet_id == 666020888022790149].head(5)


# #### Clean
# #####  resturcture the file image_pred 
# values are being used as columns names e.g. p1, p1_conf, p1_dog etc.

# In[139]:


# code
# df_image_pred - restructure the file
# p1 to p3 into one column for type and rating and boolean 
df_backup = dfc_image_pred.copy()


df1= pd.melt(dfc_image_pred,id_vars=['tweet_id','jpg_url','img_num']
        ,var_name=['ranking']
        ,value_vars=['p1','p2','p3']
        ,value_name='dog_type'
       ).sort_values('tweet_id')
        
df2= pd.melt(dfc_image_pred,id_vars=['tweet_id','jpg_url','img_num']
       ,var_name=['ranking']
       ,value_vars=['p1_conf','p2_conf','p3_conf']        
       ,value_name='confidence_of_prediction'
       ).sort_values('tweet_id')
# clean ranking

df2.ranking = df2.ranking.str[:2]
#This one is a bit slow
# df2.ranking = df2.ranking.apply(lambda x: df2.ranking.str.split("_",n=1).str[0]) 

df3= pd.melt(dfc_image_pred,id_vars=['tweet_id','jpg_url','img_num'],
       value_vars=['p1_dog','p2_dog','p3_dog']
        ,var_name=['ranking']
       ,value_name='is_dog'
       ).sort_values('tweet_id')
# clean ranking
df3.ranking = df3.ranking.str[:2]

# Merge data frames 
df_new = pd.merge(df1,df2, how='left'
                  , on=('tweet_id','jpg_url','img_num','ranking')
                  
                 )
dfc_image_pred= pd.merge(df_new,df3, how='left'
                 , on=('tweet_id','jpg_url','img_num','ranking')
                ).sort_values('tweet_id')

# df2.head(10)
# df_image.head(10)
# df_image.sample(10)
# df_image.head(20)
# df2.head()
# df_image.shape
# df.dtypes


# In[140]:


# test
# df1[df1.tweet_id==666020888022790149].head(10)
dfc_image_pred.tail(10)
# df_image.shape


# #### Clean
# #####  remove records where no dog was identified

# In[141]:


# dfc_image_pred: remove all records that are not dogs i.e. is_dog = False
print("Number of records by variable 'is_dog':\n",dfc_image_pred.is_dog.value_counts())


# In[142]:


# code
# dfc_image_pred: remove all records that are not dogs i.e. is_dog = False
dfc_image_pred = dfc_image_pred[dfc_image_pred.is_dog != False]

# test
print("Number of records by variable 'is_dog':\n",dfc_image_pred.is_dog.value_counts())


# #### Clean
# #####  Keep only the tweets that appear in the main twitter file

# In[143]:


# dfc_twitter_file: Keep only the list of tweets from the twitter file
# Thereby, removing all non necessary rows of data

print("Number of rows currently in the data before we remove the non essential rows:",dfc_image_pred.shape)


# In[144]:


# code
# dfc_twitter_file: Keep only the list of tweets from the twitter file
dfc_image_pred= dfc_image_pred[dfc_image_pred.tweet_id.isin(s_tweet_ids)]

# test
print("Number of rows after we keep only the tweet ids that match what we have in the twitter file:",dfc_image_pred.shape)


# #### Clean
# #####  change tweet_id to object

# In[145]:


# code
# change tweet_id to object
dfc_image_pred['tweet_id'] = dfc_image_pred.tweet_id.astype(np.object)


# In[146]:


# test
# check the types of the dfc_image_pred dataframe
dfc_image_pred.dtypes


# ### Clean Data

# 
# ##### twitter_json
# - Remove retweets (use (retweet) tweet_id list from twitter file) - maybe
# - Change the *_count columns to integers from objects & the tweet_id col to integer
# - Move count columns to the twitter file dataframe

# In[147]:


print("Shape of the json data:\n",dfc_json.shape)

dfc_json.dtypes


# #### Clean
# #####  change dtypes
# - tweet_id = object
# - retweet_count = int
# - favorite_count = int

# In[253]:


# Change data types of dfc_json file

dfc_json.tweet_id = dfc_json.tweet_id.astype(np.object)
dfc_json.retweet_count = dfc_json.retweet_count.astype(np.int64)
dfc_json.favorite_count = dfc_json.favorite_count.astype(np.int64)


print("Data types after the change:\n")
dfc_json.dtypes


# In[254]:


dfc_json.info()
dfc_json.sample(3)


# #### Clean
# #####  Keep only the tweets that appear in the main twitter file

# In[192]:


# dfc_json: Keep only the list of tweets from the twitter file
# print("Number of rows before we keep only the tweet ids that match what we have in the twitter file:",dfc_json.shape)

# dfc_json1= dfc_json[dfc_json.tweet_id.isin(s_tweet_ids)]

# print("Number of rows after we keep only the tweet ids that match what we have in the twitter file:",dfc_json1.shape)

# This will be taken care of during the join


# ### Clean data

# ##### twitter_file
# - add columns from json file to main twitter_file (use fillna to fill in missing values)
# - add columns from image pred file to main twitter_file (use fillna to fill in missing values)
# 

# In[234]:


dfc_json.dtypes
dfc_twitter_file.dtypes


# In[262]:


# df1 = pd.concat([dfc_twitter_file, dfc_json], axis=1, join='inner')
# dfc_twitter_file.tweet_id.dtype
# dfc_json.tweet_id.dtype
df_t = dfc_twitter_file.copy()
df_t.set_index('tweet_id')
df_t.info()
df_i = dfc_image_pred.copy()
df_i.set_index('tweet_id')
df_i.info()
df_j = dfc_json.copy()
df_j.set_index('tweet_id')
df_j.info()

# df_t.join([df_i,df_j]).info()

df = pd.concat([df_t,df_i,df_j], axis=1, join='inner')


# In[283]:


df.sample(4)


# In[263]:


# Join data from image_pred df to twitter_file df

# print("Number of columns on the twitter_file dataframe before adding two more:\n",dfc_twitter_file.shape)
# df1= pd.merge(dfc_twitter_file,dfc_json, how='left'
#                  , on='tweet_id'
#                 ).sort_values('tweet_id')

# df1.info()


# In[ ]:





# In[264]:


# df1.info()


# In[ ]:





# In[266]:


# code
# Join data from json df to twitter_file df

# print("Number of columns on the twitter_file dataframe before adding two more:\n",dfc_twitter_file.shape)
# df= pd.merge(df1,dfc_json, how='left'
#                  , on='tweet_id'
#                 ).sort_values('tweet_id')

# print("Number of missing retweet counts:",df.retweet_count.isnull().sum())
# print("Number of missing favourite counts:",df.favorite_count.isnull().sum())


# In[267]:


# Fill in missing values using mean
# df.retweet_count = df.retweet_count.fillna(df.retweet_count.mean())
# df.favorite_count = df.favorite_count.fillna(df.favorite_count.mean())

# # test
# print("Number of missing retweet counts:",df.retweet_count.isnull().sum())
# print("Number of missing favourite counts:",df.favorite_count.isnull().sum())


# In[268]:


# df.info()


# In[ ]:





# In[ ]:





# In[265]:


# change data type of the two columns added


# df.retweet_count = df.retweet_count.astype(np.int64).fillna(df.mean())
# df.favorite_count = df.favorite_count.astype(np.int64).fillna(df.mean())

# df.dtypes


# ### Store

# In[290]:


# store the clean DataFrame(s) in a CSV file with the main one named twitter_archive_master.csv
df.to_csv('twitter_archive_master.csv')


# In[72]:


# store the image prediction data in a csv file
# dfc_image_pred.to_csv('image_prediction_data.csv')


# ### Analysis

# In[285]:


print("The tweets analysed take place between", df['timestamp'].min(),"and",df['timestamp'].max())


# In[269]:


# number of tweets by month

df['timestamp'].groupby([df.timestamp.dt.year.rename('year'), df.timestamp.dt.month.rename('month')]).agg('count')


# Looking at the above cell, it would seem that interest in WeRateDogs was higher in 2015, and since then until August 2017, interest has been waining. We see a sharp drop between 2015-12 and 2016-01 followed by another sharp drop from 2016-03 to 2016-04. Since 2016-04 the number of tweets per month ave been steady. 
# 

# In[270]:


# The hour of day that tweets happen (Visual)
df['timestamp'].groupby([df.timestamp.dt.hour.rename('hour of day')]).agg('count')


# - it would seem that tweeting in the early hours of the morning is very popular - maybe people review their dog pictures and before going to sleep they tweet
# - strangley, no tweet's take place between 7am to 1pm

# In[271]:


# how many people identify their type of dog
df.dog_stage.value_counts()
ds_percent = 1 - df[df.dog_stage==''].dog_stage.agg('count') / df.shape[0]
print("Only",'{:.1%}'.format(ds_percent),"tweets identify the stage their dog is in")


# In[272]:


print('Total tweets:','{:,}'.format(df.shape[0]))
print('Total retweet count:', '{:,}'.format(df['retweet_count'].sum()))
print('Total favourite count:', '{:,}'.format(df['favorite_count'].sum()))


# - Despite the relatively low number of tweets, the reach is extensive. 
# - Over 5m have retweeted the 2k tweets on file and those 2k tweets have been favourited over 17m times
# 

# ### Visualisation

# In[273]:


import matplotlib.pyplot as plt

# df.plot(..., xticks=<your labels>)
# plt.plot(['retweet_count'], xticks=[1,2,3,4])
# df.plot.scatter(x='photo_per_tweet',y=["favorite_count","retweet_count"])
# df.plot.hist(x='timestamp',y=['retweet_count','favorite_count'])

print("chart: number of retweets and favourites based on dog stage")
df_by_stage = df[df.dog_stage!='']
df_by_stage = df_by_stage.loc[:,['dog_stage','favorite_count','retweet_count']]
df_by_stage = df_by_stage.groupby(by=df_by_stage.dog_stage).agg('sum')
df_by_stage = df_by_stage.reset_index()
df_by_stage = df_by_stage.rename(columns={"index":"dog_stage"})
ax = df_by_stage.plot(xticks=df_by_stage.index)
ax.set_xticklabels(df_by_stage["dog_stage"],rotation=90);
plt.show()

print("chart: number of retweets and favoutires based on Rating Numerator")
df_numerator = df[df.rating_numerator < 20]
df_numerator = df_numerator.loc[:,['rating_numerator','favorite_count','retweet_count']]
df_numerator = df_numerator.groupby(by=df_numerator.rating_numerator).agg('sum')
df_numerator = df_numerator.reset_index()
df_numerator = df_numerator.rename(columns={"index":"rating_numerator"})
# df_numerator.rating_numerator = df_numerator.rating_numerator.astype(np.object)
ax = df_numerator.plot(xticks=df_numerator.index)
ax.set_xticklabels(df_numerator["rating_numerator"]);
plt.show()


#sort_values('favorite_count',ascending=False)
# df_by_name


# In[275]:


# dfc_image_pred.head(5)


# In[276]:


# dfip = dfc_image_pred.copy()
# dfip.dtypes


# In[277]:


print("chart: number of retweets and favoutires based on type of dog")
df_type = df[df.confidence_of_prediction > 0.9]

df_type = df_type.loc[:,['tweet_id','dog_type','confidence_of_prediction']]

df_type.sample(3)
df_type.dog_type.value_counts().sum()
# df_type.dtypes


# In[278]:


# random checks/analysis
df_photo = df.copy()
df_photo_rc = df.copy()

df_photo = df.loc[:,['photo_per_tweet','favorite_count','retweet_count']]
df_photo = df_photo.groupby(by=df_photo.photo_per_tweet).agg('sum')

df_photo_rc = df.loc[:,['photo_per_tweet']]
df_photo_rc = df_photo_rc['photo_per_tweet'].value_counts()


print(df_photo)
print(df_photo_rc)


# In[ ]:





# In[286]:


df.dtypes


# - Retweets and Favourite counts tend to follow a pattern

# In[289]:


# A word cloud of words in the tweets
text = df.dog_type.values

wordcloud = WordCloud(
    width = 1000,
    height = 10000,
    background_color = 'white',
    stopwords = STOPWORDS).generate(str(text))
fig = plt.figure(
    figsize = (40, 30),
    facecolor = 'k',
    edgecolor = 'k')
plt.imshow(wordcloud, interpolation = 'bilinear')
plt.axis('off')
plt.tight_layout(pad=0)
plt.show()


# In[284]:


# A word cloud of words in the tweets
text = pd.read_csv('twitter_archive_master.csv', usecols=['text'])

wordcloud = WordCloud(width = 9000,
    height = 2000,
    background_color = 'black',
    stopwords = STOPWORDS).generate(' '.join(text['text']))
fig = plt.figure(
    figsize = (40, 30),
    facecolor = 'k',
    edgecolor = 'k')
plt.imshow(wordcloud, interpolation = 'bilinear')
plt.axis('on')
plt.tight_layout(pad=0)
plt.show()


# In[281]:


# A word cloud of words for the dog names
text = pd.read_csv('twitter_archive_master.csv', usecols=['name'])


wordcloud = WordCloud(width = 3000,
    height = 2000,
    background_color = 'black',
    stopwords = STOPWORDS).generate(' '.join(text['name']))
fig = plt.figure(
    figsize = (40, 30),
    facecolor = 'k',
    edgecolor = 'k')
plt.imshow(wordcloud, interpolation = 'bilinear')
plt.axis('on')
plt.tight_layout(pad=0)
plt.show()


# In[282]:



df.dtypes


# In[ ]:





# In[ ]:




