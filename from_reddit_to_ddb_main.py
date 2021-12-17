import praw
import boto3
from from_reddit_to_ddb_scripts import awards_iterator
from from_reddit_to_ddb_scripts import comments_iterator
from from_reddit_to_ddb_scripts import create_reddit_news_table_ddb
from from_reddit_to_ddb_scripts import load_news_to_ddb
from from_reddit_to_ddb_scripts import submission_iterator
from from_reddit_to_ddb_scripts import open_json_file
import json
from from_reddit_to_ddb_params import reddit_credentials
from from_reddit_to_ddb_params import number_of_news
from decimal import Decimal
from from_reddit_to_ddb_params import what_we_do
from from_reddit_to_ddb_scripts import load_news_to_firestore
reddit = reddit_credentials

# Tech data
news_counter = 1



if what_we_do == 1:
    # Creates table if it was not created yeat
    create_reddit_news_table_ddb()

elif what_we_do == 2:
    #upload json file to ddb
    news_list = open_json_file()
    load_news_to_ddb(news_list)

elif what_we_do == 3:
    # save news to DDB
    subreddit = reddit.subreddit('news').hot(limit=number_of_news)

    for submission in subreddit:
        awards = awards_iterator(submission)
        comments = comments_iterator(submission)
        news_list_unclean = submission_iterator(submission, awards, comments)
        news_list = json.loads(json.dumps(news_list_unclean), parse_float=Decimal)
        print('News # ' + str(news_counter ) + ' of '+ str(number_of_news)+ ' saved')
        news_counter += 1

    load_news_to_ddb(news_list)

elif what_we_do == 4:
    # upload json file to firestore
    news_list = open_json_file()
    load_news_to_firestore(news_list)

elif what_we_do == 5:
    # save news to firestore
    subreddit = reddit.subreddit('news').hot(limit=number_of_news)

    for submission in subreddit:
        awards = awards_iterator(submission)
        comments = comments_iterator(submission)
        news_list_unclean = submission_iterator(submission, awards, comments)
        news_list = news_list_unclean
        print('News # ' + str(news_counter) + ' of ' + str(number_of_news) + ' saved')
        news_counter += 1


    load_news_to_firestore(news_list)

else:
    print('inappropriate action')


print("Done")