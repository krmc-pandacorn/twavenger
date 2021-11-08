import sys
import argparse

consumer_key = ''
consumer_secret = ''
query = ''

def main():
    args = sys.argv[1:]

    #Parse command line
    parser = argparse.ArgumentParser(description='Collect many tweets.')
    parser.add_argument('--consumer_key', metavar='N', type=str, dest='consumer_key',
                        help='Twitter API consumer key')
    parser.add_argument('--consumer_secret', metavar='N', type=str, dest='consumer_secret',
                        help='Twitter API consumer secret')
    parser.add_argument('--query', metavar='N', type=str, dest='query',
                        help='Twitter API query string')

    args = parser.parse_args()


    #Get Twitter keys
    consumer_key = args.consumer_key
    consumer_secret = args.consumer_secret
    query = args.query

if __name__ == "__main__":
    main()


import twarc
from twarc.client2 import Twarc2
from urllib.error import HTTPError
import pandas as pd
import time
from datetime import datetime, timedelta
import json


def get_twarc_connection():
    return twarc.Twarc2(
        consumer_key = consumer_key, 
        consumer_secret = consumer_secret
    )

def collect_tweets(year, 
                   month, 
                   day_of_month, 
                   twarc_ref, 
                   max_days = 1,
                   tweets_to_collect = 60000, 
                   tweets_per_page = 100,
                   q = query,
                   sleepseconds = 3):
    
    tweet_list = []

    start = datetime.strptime(f'{year}-{month:02d}-{day_of_month:02d}', '%Y-%m-%d')
    end = start + timedelta(days=max_days)
    
    print(f'Start date: {start}')
    print(f'End date: {end}')
    
    print(f'Executing query "{q}""')

    pages = twarc_ref.search_all(query=q, 
                                     start_time=start,
                                     end_time=end,
                                     max_results=tweets_per_page)
    
    print(f'Retrieving pages')
    
    byonek = 3000

    for page in pages:

        tweets = page.get('data')

        for tweet in tweets:
            tweet_list.append(tweet)
            
        tweets_so_far = len(tweet_list)
            
        if tweets_so_far > byonek:
            print(f'Collected {tweets_so_far} tweets')
            byonek+=3000
            
        if tweets_so_far >= tweets_to_collect:
            break;

        #avoid making too many requests
        time.sleep(sleepseconds)
            

    #Return tweets
    print(f'Returning {len(tweet_list)} tweets')
    return tweet_list



for year in range(2020, 2022):
    
    year_tweets = []
    for month in range(0,12):

        try:
            month_tweets = collect_tweets(year, month+1, 10, T)
        except HTTPError as err:
            if err.code == 401:
                #deal with expired token by renewing
                print('HTTP 401 error; renewing token')
                T = get_twarc_connection()
                #try again
                month_tweets = collect_tweets(year, month+1, 10, T)

        #we're creating one long list of tweets
        year_tweets.extend(month_tweets)

        #Save monthy data so's not to lose progress
        filename = f'./tweet_month_data_{year}_{month}.json'
        with open(filename, 'w') as outfile:
            json.dump(month_tweets, outfile) 

        time.sleep(3)

        print(f'Tweets collected: {len(year_tweets)}')

    filename = './tweet_raw_data_' + str(year) + '.json'
    with open(filename, 'w') as outfile:
        json.dump(year_tweets, outfile)   



T = get_twarc_connection()


