#!/usr/bin/python
import praw
import re
import sys
import boto3
import json
import time
import textstat
from textblob import TextBlob
from textblob import Blobber
from better_profanity import profanity
from time import sleep
from datetime import datetime, timezone

firehose_client = boto3.client('firehose', region_name="us-east-1")
#sns_client = boto3.client('sns', region_name="us-east-1")
#sns_targetARN = "<insert-sns-target-arn>"

bot_list = ['AutoModerator', 'keepthetips', 'MAGIC_EYE_BOT',
            'Funny_Sentinel', 'Funny-Mod', 'Showerthoughts_Mod', 'autotldr',
            'art_moderator_bot', 'ApiContraption', 'WSBVoteBot', 'FittitBot',
            'Photoshopbattlesbot', 'dataisbeautiful-bot', 'timestamp_bot',
            'remindditbot', 'converter-bot', 'lntipbot']

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

def send_batch_to_firehose(comments_batch):
    try:
        response = firehose_client.put_record_batch(
            DeliveryStreamName='<insert-delivery-stream-name>',
            Records=comments_batch
        )
    except Exception as e:
        print(str(e))

def send_record_to_firehose(comment):
    try:
        response = firehose_client.put_record(
            DeliveryStreamName='<insert-delivery-stream-name>',
            Record={
                'Data': (json.dumps(comment, ensure_ascii=False) + '\n').encode('utf8')
                    }
            )
    except Exception as e:
        print(str(e))

# Send alert to sns topic
def send_sns_alert(message, subject):
    # Publish a simple message to the specified SNS topic
    sns_client.publish(
        TargetArn=sns_targetARN,
        Message=str(message),
        Subject=str(subject)
    )

def remove_emoji(comment):
    emoji_pattern = re.compile("["
       u"\U0001F600-\U0001F64F"  # emoticons
       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
       u"\U0001F680-\U0001F6FF"  # transport & map symbols
       u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
       u"\U00002702-\U00002f7B0"
       u"\U000024C2-\U0001F251"
       "]+", flags=re.UNICODE)

    cleaned_comment =  emoji_pattern.sub(r'', comment)

    return cleaned_comment

def get_comment_sentiment(comment):
    pattern_analysis = TextBlob(comment)
    return pattern_analysis.sentiment

def get_comment_language(comment):
    language = ''
    if (len(comment) > 3):
        language = TextBlob(comment).detect_language()
    return language

if len(sys.argv) >= 2:

    while True:

        begin_msg = "starting comment stream... "
        #send_sns_alert(begin_msg, "comment stream initiation")

        try:
            r = praw.Reddit('bot1')
            num_comments_collected = 0
            comments_processed = 0
            comments_batch = []
            batch_delivery = False

            # build stream. add first subreddit to start.
            subreddits = sys.argv[1]
            for sr in sys.argv[2:]:
                subreddits = subreddits + "+" + sr

            comment_stream = r.subreddit(subreddits)

            for comment in comment_stream.stream.comments():

                # throttle to avoid 429 error
                sleep(0.5)

                # empty check
                if (comment):
                    commentbody = comment.body
                    author = comment.author

                    if author not in bot_list:

                        if (len(commentbody) > 0 and len(commentbody) < 5000):

                            #censor check
                            if profanity.contains_profanity(str(commentbody)):
                                is_censored = 1
                            else:
                                is_censored = 0

                            # remove emojis
                            cleaned_comment = remove_emoji(str(commentbody))

                            # comment date
                            comment_date = str(datetime.utcfromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S'))

                            #compartmentalize and localize date for easier searching
                            dt = utc_to_local(datetime.strptime(comment_date, '%Y-%m-%d %H:%M:%S'))
                            comment_timestamp = dt.strftime('%Y/%m/%d %H:%M:%S')

                            # comment sentiment and subjectivity
                            sentiment = get_comment_sentiment(cleaned_comment)
                            pattern_polarity = round(sentiment.polarity,4)
                            pattern_subjectivity = round(sentiment.subjectivity, 4)

                            is_positive = 0
                            is_neutral = 0
                            is_negative = 0

                            if (pattern_polarity > 0.3):
                                is_positive = 1
                            elif (pattern_polarity >= -0.3 and pattern_polarity <= 0.3):
                                is_neutral = 1
                            else:
                                is_negative = 1

                            is_subjective = 0
                            if (pattern_subjectivity > 0.7):
                                is_subjective = 1

                            # language
                            comment_language = get_comment_language(cleaned_comment)

                            # Readability statistics
                            comment_reading_ease_score = textstat.flesch_reading_ease(cleaned_comment)
                            comment_reading_ease = ''
                            if (comment_reading_ease_score >= 80):
                                comment_reading_ease = 'easy'
                            elif (comment_reading_ease_score > 50 and comment_reading_ease_score < 80):
                                comment_reading_ease = 'standard'
                            else:
                                comment_reading_ease = 'difficult'

                            comment_reading_grade_level = textstat.text_standard(cleaned_comment, float_output=False)

                            # censor and lower
                            censored_comment = profanity.censor(cleaned_comment).lower()

                            commentjson = {
                                            'comment_id': str(comment),
                                            'subreddit': str(comment.subreddit),
                                            'author': str(comment.author),
                                            'comment_text': censored_comment,
                                            'distinguished': comment.distinguished,
                                            'submitter': comment.is_submitter,
                                            'total_words': len(cleaned_comment.split()),
                                            'reading_ease_score': comment_reading_ease_score,
                                            'reading_ease': comment_reading_ease,
                                            'reading_grade_level': comment_reading_grade_level,
                                            'sentiment_score': pattern_polarity,
                                            'censored': is_censored,
                                            'comment_language': comment_language,
                                            'positive': is_positive,
                                            'neutral': is_neutral,
                                            'negative': is_negative,
                                            'subjectivity_score': pattern_subjectivity,
                                            'subjective': is_subjective,
                                            'url': "https://reddit.com" + comment.permalink,
                                            'comment_date': comment_date,
                                            'comment_timestamp': comment_timestamp,
                                            'comment_hour': dt.hour,
                                            'comment_year': dt.year,
                                            'comment_month': dt.month,
                                            'comment_day': dt.day
                                        }

                            comments_processed = comments_processed + 1

                            # print("==================================")
                            num_comments_collected = num_comments_collected + 1
                            # print(num_comments_collected)
                            # print(commentjson)

                            # If batch_delivery is true, deliver to firehose in batches of 500 records. Otherwise, send each record individually
                            if(batch_delivery):
                                # add comment to the batch
                                comments_batch.append(
                                    {
                                        'Data': (json.dumps(commentjson, ensure_ascii=False) + '\n').encode('utf8')
                                    }
                                )

                                #send batches of 500 comments
                                if (comments_processed % 500 == 0):
                                    print("sending batch to firehose...")
                                    send_batch_to_firehose(comments_batch)
                                    print("Comments sent to firehose: ", comments_processed)
                                    comments_batch = []
                                    print("emptied batch...")
                            else:
                                send_record_to_firehose(commentjson)

        except Exception as e:
            print(str(e))
            error_msg = " An error has occured in the comment stream:" + str(e)
            curr_date = datetime.now().strftime("%m/%d/%Y")
            subject = "Reddit App Alert: "+ str(curr_date)

            # If too many requests error, we need to wait longer for throttle to end. otherwise start back up right away.
            error_code = "".join(filter(str.isdigit, str(e)))
            http_response = int(error_code)
            if (http_response == 429):
                error_msg = error_msg + " - Too many requests. Restarting stream in 2 hours."
                #send_sns_alert(error_msg, subject)
                sleep(7200)
            else:
                error_msg = error_msg + " - Restarting stream now."
                #send_sns_alert(error_msg, subject)

else:
    print("please enter subreddit.")



