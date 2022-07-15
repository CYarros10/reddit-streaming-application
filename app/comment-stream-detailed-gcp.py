#!/usr/bin/python
import praw
import re
import sys

import json
import time
import textstat
from textblob import TextBlob
from textblob import Blobber
from better_profanity import profanity
from time import sleep
from datetime import datetime, timezone

# ----- GCP -----
import os
from google.cloud import pubsub_v1
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="key.json"

project_id = "<insert>"
topic_id = "<insert>"

# Configure the batch to publish as soon as there are 10 messages
# or 1 KiB of data, or 1 second has passed.

batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=10,  # default 100
    max_bytes=1024,  # default 1 MiB
    max_latency=1,  # default 10 ms
)
publisher = pubsub_v1.PublisherClient(batch_settings)
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []

def push_payload(payload):              
        data = json.dumps(payload).encode("utf-8")           
        future = publisher.publish(topic_path, data=data)
        print("Pushed message to topic.")

# ---------------

bot_list = ['AutoModerator', 'keepthetips', 'MAGIC_EYE_BOT',
            'Funny_Sentinel', 'Funny-Mod', 'Showerthoughts_Mod', 'autotldr',
            'art_moderator_bot', 'ApiContraption', 'WSBVoteBot', 'FittitBot',
            'Photoshopbattlesbot', 'dataisbeautiful-bot', 'timestamp_bot',
            'remindditbot', 'converter-bot', 'lntipbot']


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


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
                            num_comments_collected = num_comments_collected + 1
                            print(num_comments_collected)
                            push_payload(commentjson)

        except Exception as e:
            error_msg = " An error has occured in the comment stream:" + str(e)
            print(error_msg)

            # If too many requests error, we need to wait longer for throttle to end. otherwise start back up right away.
            error_code = "".join(filter(str.isdigit, str(e)))
            http_response = int(error_code)
            if (http_response == 429):
                error_msg = error_msg + " - Too many requests. Restarting stream in 2 hours."
                sleep(7200)
            else:
                error_msg = error_msg + " - Restarting stream now."
else:
    print("please enter subreddit.")
