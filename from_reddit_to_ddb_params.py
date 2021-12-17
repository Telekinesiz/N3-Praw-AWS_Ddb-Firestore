import praw
import boto3
import os
from firebase_admin import credentials

#Parameters**********************************************************
number_of_news = 3
table_name = "Reddit_news"
file_name = "Reddit.json"
what_we_do = 3      # paramateres for this: 1 - create ddb table
                    # 2 - upload file to ddb
                    # 3 - save news to ddb
                    # 4 -upolad file to firestore
                    # 5 - upload news to firestore


#credentials*********************************************************

reddit_credentials = praw.Reddit(
    user_agent=os.getenv('praw_user_agent'),
    client_id=os.getenv('praw_client_id'),
    client_secret=os.getenv('praw_client_secret'),
    username=os.getenv('praw_username'),
    )

dynamodb_credentials = boto3.resource('dynamodb', region_name='eu-central-1',
                                  aws_access_key_id=os.getenv('aws_access_key_id'),
                                  aws_secret_access_key=os.getenv('aws_secret_access_key'))

cred = credentials.Certificate("serviceAccountKey.json")

if __name__ == '__main__':
    print('praw_user_agent ' + os.getenv('praw_user_agent'))
    print('praw_client_id ' + os.getenv('praw_client_id'))
    print('praw_client_secret ' + os.getenv('praw_client_secret'))
    print('praw_username ' + os.getenv('praw_username'))
    print('aws_access_key_id ' + os.getenv('aws_access_key_id'))
    print('aws_secret_access_key ' + os.getenv('aws_secret_access_key'))
