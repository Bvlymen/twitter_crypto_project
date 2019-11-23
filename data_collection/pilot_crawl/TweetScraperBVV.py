
# coding: utf-8
"""
Example Command line call to use this code:
python '\Path\To\Folder\TweetScraperBVV.py' --database=CryptoCrawling --startdate=2017-01-01 --enddate=2019-10-01 -- --filters=bitcoin,ethereum,btc,eth --chunkminutes=20  --password=secretpassword --username=cryptocrawlee --database=CryptoCrawling --tablename=crypto_tweets
"""


# In[130]:


import pandas as pd
import sqlalchemy
import numpy as np
from sqlalchemy_utils.functions import create_database
import datetime as dt
from twitterscraper import query_tweets
import subprocess
import sys
import re
from twitterscraper.query import query_single_page
import math

import sys
import getopt

#Supress command line warnings and info
import warnings
warnings.filterwarnings("ignore")


# In[194]:


fullCmdArguments = sys.argv

# -Specify arguments to be taken from command line
argumentList = fullCmdArguments[1:]
unixOptions = "f:d:u:p:c:s:e:q:t:"
gnuOptions = ['file=','database=','username=','password=','chunkminutes=','startdate=','enddate=','filters=','table=']

try:
    arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    argdict = dict(arguments)
except getopt.error as err:
    # output error, and return with an error code
    print (str(err))
    sys.exit(2)

# In[200]:


### Parse the command line arguments
try:
    password = str(argdict['--password'])
except:
    password = 'secretpassword'
try:
    username = str(argdict['--username'])
except:
    username = 'cryptocrawlee'
try:
    databasename = str(argdict['--database'])
except:
    databasename = 'CryptoCrawling'
try:
    filters = str(argdict['--filters']).split(',')
except:
    filters = ['Bitcoin','BTC']
try:
    startdate = str(argdict['--startdate'])
except:
    startdate = '2019-01-01'
try:
    enddate = str(argdict['--enddate'])
except:
    enddate= '2019-01-02'
try:
    chunkminutes = int(argdict['--chunkminutes'])
except:
    chunkminutes=20
try:
    tablename = str(argdict['--table'])
except:
    tablename = 'crypto_tweets'

print(tablename,startdate,enddate)


# In[201]:


### Helper functions for chunking tweet collection

def get_startend_tweet_ids(filters=['bitcoin'], curday = dt.datetime.today(), next_day = dt.datetime.today() + dt.timedelta(days=1)):
#### Get IDs of tweet at start of day and end of day to be able to chunk tweet queries ###

    # combine filters into API query string
    filters = [fil.replace('#','%23') for fil in filters] 
    query_filters = str('%20OR%20'.join(filters))

    #### Get ID for both start and enddate
    #initalise list to hold IDs
    tweet_ids = []

    dates = [curday, next_day]
    for date in dates:

        since_date=date-dt.timedelta(days=1) # get date of day before
        until_date=date # current date

        #convert dates into twitter readable (iso-)format
        since_datestr = since_date.isoformat()[:10]
        until_datestr = until_date.isoformat()[:10]

        #create twitter query string
        query = query_filters + '%20since%3A'+since_datestr+'%20until%3A'+until_datestr+'&src=typd'

        ### Get last tweets of day before
        last_tweets_of_day = query_single_page(query=query,lang='en',pos=0)

        #Select very last tweet of day and get it's ID
        last_tweet_of_day= last_tweets_of_day[0][0]
        last_tweetid = last_tweet_of_day.tweet_id

        #Add ID to list
        tweet_ids.append(last_tweetid)
    
    tweet_ids =[int(tweet_id) for tweet_id in tweet_ids]
    return tweet_ids

def calculate_interval_tweetids(daystart_id, dayend_id, interval_mins):
#################### calculate tweet id pairs needed to collect correct tweets at each sub-timeperiod
    # What period length do you want to chunk collect tweets into (in minutes)
    interval_mins=20

    #using start and end of day tweet ids, calculate the jump in ID between x minute periods
    id_interval = int((dayend_id - daystart_id)/((60*24)/interval_mins))

    #Calculate all the min/max tweet ids of each x minute period
    all_ids= np.linspace(start=daystart_id, stop=dayend_id, retstep= ((60*24)/interval_mins)-1 )[0]
    
    #categorise them into start of interval ids or end of interval ids
    since_ids = [int(tweetid) for tweetid in all_ids[:-1]]
    max_ids = [int(tweetid) for tweetid in all_ids[1:]]
   
    return since_ids, max_ids


# In[202]:


result = subprocess.run("bash -c 'service mysql status'", stdout=subprocess.PIPE)
print('Query returned:\n',result.stdout.decode())
if len(re.findall('stopped',result.stdout.decode(), flags=re.IGNORECASE))>0:
    print('MySQL is installed but may not be running.\n')
elif len(re.findall('unrecognized',result.stdout.decode(), flags=re.IGNORECASE))>0:
    print('MySQL is not installed\n Warning: MySQL Should be installed before proceeding\n')
else:
    print('Unknown as to whether MySQl is installed or not\n')


# In[204]:


print("Attempting to establish connection to database: ", databasename, "with username:", username, "using password:",password,"\n")
try:
    create_database('mysql://cryptocrawlee:secretpassword@127.0.0.1/CryptoCrawling')
    print('Database did not exist so it was created instead')
except:
    print('Database already exists - continuing.')
conn = sqlalchemy.create_engine('mysql://'+username+':'+password+'@127.0.0.1/'+databasename) # connect to server

print('Connection to database established\n')


# In[199]:


# conn.execute('DROP TABLE crypto_tweets')


# In[205]:


sqlquery = "CREATE TABLE IF NOT EXISTS "+ tablename + """
(
timestamp DATETIME,
screen_name VARCHAR(200),
text VARCHAR(1000),
likes INT,
retweets INT,
replies INT,
hashtags VARCHAR(500),
username VARCHAR(200),
is_replied INT,
is_reply_to INT,
has_media INT,
img_urls VARCHAR(500),
links VARCHAR(500),
timestamp_epochs BIGINT,
tweet_id BIGINT ,
tweet_url VARCHAR(150),
user_id BIGINT,
parent_tweet_id BIGINT,
reply_to_user_names VARCHAR(300),
reply_to_user_ids VARCHAR(200)
)
 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci
"""


# In[206]:


create_table_return = conn.execute(sqlquery)
print("Table:", tablename, "created if not already in existence\n")


# In[207]:


def crawl_tweets_toSQL(filters = ['bitcoin', 'btc'], startdate='2019-01-01', enddate='2019-01-02', chunkminutes =20):
    
    numintervals = math.ceil((60*24)/20)
    print('Collecting tweets in',str(numintervals),'chunks each day')
    
    #create a list of all dates we want to collect tweets from
    daterange = pd.date_range(start = startdate, end=enddate, freq='d')
    
    for date in daterange:
    
        #### Calculate tweet IDs to be able to chunk/break up tweet collection into smaller blocks
        ##calculate the tweet id of the first and last tweets of a day
        #args = filters, curday, nextday
        startend_ids = get_startend_tweet_ids(filters=filters, curday = date, next_day = date + dt.timedelta(days=1))
        #Return lists of the start and end tweet ids for each chunk of tweets
        chunkstart_ids, chunkend_ids = calculate_interval_tweetids(startend_ids[0], startend_ids[1], 20)

        for idx, (startid, endid) in enumerate(zip(chunkstart_ids, chunkend_ids)):
            
            query_filters = str(' OR '.join(filters))
            query = query_filters + ' since_id:'+str(startid)+' max_id:'+str(endid)
            tweets = query_tweets(query=query,
                                  limit=None,
                                  begindate=date.date(),enddate=(date+dt.timedelta(days=1)).date(),
                                  lang='en')

            tweet_table =[]

            for tweet in tweets:

                timestamp  = tweet.timestamp.isoformat() 
                screen_name = tweet.screen_name.encode('utf-8')
                text = tweet.text.encode('utf-8')
                likes = tweet.likes
                retweets = tweet.retweets
                replies = tweet.replies
                hashtags = str(tweet.hashtags).encode('utf-8')
                username = tweet.username.encode('utf-8')
            #     text_html = tweet.text_html.encode('utf-8')
                is_replied = int(tweet.is_replied)
                is_reply_to = int(tweet.is_reply_to)
                has_media = int(tweet.has_media)
                img_urls = str(tweet.img_urls).encode('utf-8')
                links = str(tweet.links).encode('utf-8')
                timestamp_epochs = tweet.timestamp_epochs
                tweet_id = tweet.tweet_id
                tweet_url = tweet.tweet_url.encode('utf-8')
                user_id = tweet.user_id
                try:
                    parent_tweet_id  = int(tweet.parent_tweet_id) 
                except:
                    parent_tweet_id = None
                reply_to_user_names = str([data_dict['screen_name'] for data_dict in tweet.reply_to_users]).encode('utf-8')
                reply_to_user_ids = str([data_dict['user_id'] for data_dict in tweet.reply_to_users]).encode('utf-8')

                tweet_data= [
                timestamp,
                screen_name,
                text ,
                likes,
                retweets,
                replies,
                hashtags ,
                username,
            #     text_html ,
                is_replied,
                is_reply_to,
                has_media,
                img_urls,
                links,
                timestamp_epochs,
                tweet_id,
                tweet_url,
                user_id,
                parent_tweet_id,
                reply_to_user_names,
                reply_to_user_ids
                ]   

                tweet_table.append(tweet_data)


            tmp = pd.DataFrame(tweet_table)



            colnames  = [

            "timestamp"   ,
            "screen_name",
            "text" ,
            "likes",
            "retweets",
            "replies",
            "hashtags" ,
            "username",
            # "text_html" ,
            "is_replied",
            "is_reply_to",
            "has_media",
            "img_urls" ,
            "links" ,
            "timestamp_epochs" ,
            "tweet_id" ,
            "tweet_url" ,
            "user_id" ,
            "parent_tweet_id" ,
            "reply_to_user_names",
            "reply_to_user_ids" 
                        ]
            tmp.columns = colnames

            tmp.to_sql(name = 'crypto_tweets', con = conn, if_exists='append', index=False)
            
            print('Added Tweets for date:', date.isoformat(), 'chunk:', str(idx+1), 'of:', str(numintervals))
        
    print('Collected all tweets from:',startdate, 'to', enddate)


# In[143]:

import logging
logging.disable(sys.maxsize)
logging.getLogger("twitterscraper").disabled = True
logging.getLogger("twitterscraper.query").disabled = True

crawl_tweets_toSQL(filters = filters, startdate=startdate, enddate=enddate, chunkminutes =chunkminutes)


# In[ ]:


#################################################

