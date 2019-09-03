#!/usr/bin/env python
# coding: utf-8

# In[186]:


# imports
import pandas as pd
import numpy as np
import matplotlib as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[ ]:


# variables



# #### Read data (3 sources: twitter file, image predictions, twitter api extra data)

# In[18]:


# Read data (3 sources: twitter file, image predictions, twitter api extra data)

# Read data - twitter file (csv)
df_twitter_file = pd.read_csv("twitter-archive-enhanced.csv")


# #### Assess Data

# ##### Summary of findings based on code below
# 
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




