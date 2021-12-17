from praw.models import MoreComments
from decimal import Decimal
import json
import boto3
from from_reddit_to_ddb_params import dynamodb_credentials
from from_reddit_to_ddb_params import table_name
from from_reddit_to_ddb_params import file_name
import firebase_admin
from firebase_admin import firestore
from from_reddit_to_ddb_params import cred

# Geting info about rewards, name and discription can be usefull for human, but for ML model i would left only id price, and count
def awards_iterator(submission):
    awards = []

    for award in submission.all_awardings:
        name = award["name"]
        id = award["id"]
        description = award["description"]
        coin_price = award["coin_price"]
        count = award["count"]

        awards.append(
            {"name": name,
             "id": id,
             "description": description,
             "coin_price": coin_price,
             "count": count}
        )
    return awards

#Receiveing info about comments. Sticked and Distinguished comments can receive additional vote because more people will be able to see it, so this parameters should be count
def comments_iterator(submission):
    comments = []
    for top_level_comment in submission.comments:
        if isinstance(top_level_comment, MoreComments):
            continue

        comments.append(
            {'Comment': top_level_comment.body,
             'Is_submitter': top_level_comment.is_submitter,
             'Score': top_level_comment.score,
             'Sticked': top_level_comment.stickied,
             'Distinguished': top_level_comment.distinguished}
        )
    return comments

#saving required infromation abot submission

submission_full_info = []
def submission_iterator(submission, awards, comments):


    submission_full_info.append(
        {'ID': submission.id,
         'Date': submission.created_utc,
         'Name': submission.title,
         'Text': submission.selftext,
         'Score': submission.score,
         'Ratio': submission.upvote_ratio,
         'Comments_num': submission.num_comments,
         'Page_url': str('https://www.reddit.com') + submission.permalink,
         'Awards': awards,
         'Comments': comments}
            )
    return submission_full_info

#Work with dynamo db

def create_reddit_news_table_ddb(dynamodb=None):
    if not dynamodb:
        dynamodb = dynamodb_credentials

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'Date',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'Name',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Date',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'Name',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    return table


def load_news_to_ddb(news_list, dynamodb=None):
    if not dynamodb:
        dynamodb = dynamodb_credentials

    table = dynamodb.Table(table_name)
    for news in news_list:
        Name = news['Name']


        print("Adding news:", Name)
        table.put_item(Item=news)


#upload news to firestore
def load_news_to_firestore(news_list):
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    for i in range(len(news_list)):
        ID = news_list[i]['ID']
        Date = news_list[i]['Date']
        Name = news_list[i]['Name']
        Text = news_list[i]['Text']
        Score = news_list[i]['Score']
        Ratio = news_list[i]['Ratio']
        Comments_num = news_list[i]['Comments_num']
        Page_url = news_list[i]['Page_url']
        Awards = news_list[i]['Awards']
        Comments = news_list[i]['Comments']
        db.collection(table_name).document(ID).set({'ID': str(news_list[i]['ID']),
                                                       'Date': str(news_list[i]['Date']),
                                                       'Name': Name,
                                                       'Text': Text,
                                                       'Score': Score,
                                                       'Ratio': str(Ratio),
                                                       'Comments_num': Comments_num,
                                                       'Page_url': Page_url,
                                                       'Date': str(Date)})

        for i in range(len(Awards)):
            name = Awards[i]["name"]
            id = Awards[i]["id"]
            description = Awards[i]["description"]
            coin_price = Awards[i]["coin_price"]
            count = Awards[i]["count"]
            db.collection(table_name).document(ID).collection('Awards').document(id).set({'name': name,
                                                                                             'id': id,
                                                                                             'description': description,
                                                                                             'coin_price': coin_price,
                                                                                             'count': count})

        for i in range(len(Comments)):
            Comment = Comments[i]["Comment"]
            Is_submitter = Comments[i]["Is_submitter"]
            Comment_Score = Comments[i]["Score"]
            Sticked = Comments[i]["Sticked"]
            Distinguished = Comments[i]["Distinguished"]
            db.collection(table_name).document(ID).collection('Comments').add({'Comment': Comment,
                                                                                  'Is_submitter': Is_submitter,
                                                                                  'Comment_Score': Comment_Score,
                                                                                  'Sticked': Sticked,
                                                                                  'Distinguished': Distinguished})

        print("Upload news " + Name)

#work with json files

def open_json_file():
    with open(file_name, "r", encoding='utf-8') as json_file:
        news_list = json.load(json_file, parse_float=Decimal)
    return (news_list)

def save_to_json(submission_full_info):
    with open(file_name, "w", encoding='utf-8') as file:
        json.dump(submission_full_info, file, indent=4, ensure_ascii=False)
        print("Done")