#!/usr/bin/env python

import json
from datetime import datetime
import re

import tweepy

# Hard-coded:
credentials_file = '/home/skems/gabinete/projetos/keys-configs/gabitwitter.json'
post_file_template = '../scripts/posts/dou_%(secao)s_%(data)s.txt'
# Tags:
tag_secao1     = '#Ato_Normativo'
tag_secao2     = '#Cargo_Alto'
tag_presidente = '#Ato_Presidencial'
tag_extra      = '#Extra'


#################
### Functions ###
#################

def load_credentials(credentials_file):
    """
    Load a JSON file `credentials_file` (str) containing twitter's
    consumer API key and secret and access token and secret into 
    a Tweepy OAuth object.
    """
    # Load credentials from file:
    with open(credentials_file, 'r') as f:
        credentials = json.load(f)
    
    # Set tweepy authentication object:
    auth = tweepy.OAuthHandler(credentials['api_key'], credentials['api_secret_key'])
    auth.set_access_token(credentials['access_token'], credentials['access_token_secret'])
    
    return auth


def title_to_tag(title):
    """
    Convert hard-coded whatsapp message titles to
    hard-coded twitter tags.
    """
    
    title_segments = ['Destaques do DOU', 'cargos altos', 'Atos do presidente']
    tags           = [tag_secao1, tag_secao2, tag_presidente, tag_extra]
    
    for segment, tag in zip(title_segments, tags):
        if title.find(segment) != -1:
            return tag
    
    raise Exception('Unknown title.')


def title_to_extra_tag(title):
    if title.find('Extra') != -1:
        return tag_extra
    else:
        return ''


def topic_to_tag(topic):
    """
    Transform a topic title in a whatsapp message into
    a twitter hashtag.
    """
    return '#' + topic.replace('*', '').replace(' ', '_').replace('.', '')


def build_tweet(title_tag, topic_tag, message, extra='', date=True):
    """
    Format one single DOU headline as a tweet by adding 
    the topic and the post title as hashtags. There is also 
    a tag to identify extra editions.
    """
    
    if len(extra) > 0:
        extra = '  ' + extra
    if date:
        today_tag = datetime.today().strftime('#%Y-%m-%d')
    else:
        today_tag = ''
        
    tweet = topic_tag + '  ' + title_tag + extra + '  ' + today_tag + '\n' + message
    
    return tweet.strip()


def count_tweet_characters(tweet, max_chars=280, link_size=23, emoji_len=2):
    """
    Raise an Exception if tweet has more than the allowed number of characters.
    """
    
    # Replace link my twitter standard link size:
    url_place_holder = 'x' * link_size
    to_count = re.sub(r'http://\S+', url_place_holder, tweet)
    
    # Count message length, assuming there is just one emoji:
    tweet_len = len(to_count) - 1 + emoji_len
    
    if tweet_len > max_chars:
        raise Exception('The following tweet exceeded character limit:\n' + tweet)


def create_topic_tweets(title_tag, topic_message, extra_tag=''):
    """
    Given a message `topic_message` (str) starting by a topic 
    header and followed by individual headlines (all separared 
    by skipping a  line), return a list of tweets to be posted. 
    A `title_tag` (str) is  also added to the tweets.
    
    Ex. of `topic_message`
    ----------------------
    
    Economia e Regulação*

    🧮 Regulamenta o regime aduaneiro especial de drawback.
    http://www.in.gov.br/en/web/dou/-/portaria-n-44-de-24-de-julho-de-2020-268684638

    🧮 Estabelece os procedimentos para registro de sindicatos junto ao Ministério da Economia.
    http://www.in.gov.br/en/web/dou/-/portaria-n-17.593-de-24-de-julho-de-2020-268684112
    """
    
    # Split topic message by vertical spaces:
    segmented_topic = topic_message.split('\n\n')
    
    # Get topic title if existing:
    if segmented_topic[0].strip().find('*', 1) == len(segmented_topic[0].strip()) -1:
        # Get topic title (tag):
        topic_tag       = topic_to_tag(segmented_topic[0])
        # Split individual headlines:
        topic_body      = segmented_topic[1:]
    
    # Case there is no topic:
    else:
        topic_tag = ''
        topic_body      = segmented_topic
    
    # Remove footnote:
    if topic_body[-1].find('Gabinete Compartilhado Acredito') != -1:
        topic_body = topic_body[:-1]
    
    # Create list of individual tweets (one for each headline):
    tweets = [build_tweet(title_tag, topic_tag, headline, extra_tag) for headline in topic_body]
    
    # Check if tweets have the appropriate length:
    for tweet in tweets:
        count_tweet_characters(tweet)
    
    return tweets


def full_message_to_tweet_list(full_message, reverse=True):
    """
    Transform a text `full_message` (str) intended to be 
    posted in whatsapp as a single message into a list of 
    tweets to be posted on twitter.
    
    If `reverse` (bool) is True, return tweets in reverse order.
    """
    
    # Get message title:
    segmented_message = full_message.split('\n\n')
    title = segmented_message[0].strip()
    title_tag = title_to_tag(title)
    # Get extra edition tag (if any):
    extra_tag = title_to_extra_tag(title)
    # Get message body:
    message_body = '\n\n'.join(segmented_message[1:])
    # Split into topics:
    message_topics = message_body.split('\n*')
    
    # Loop over topics and create list of tweets:
    tweets = []
    for topic_message in message_topics:
        topic_tweets = create_topic_tweets(title_tag, topic_message, extra_tag)
        tweets = tweets + topic_tweets
    
    # Reverse order of tweets if requested:
    if reverse:
        tweets = tweets[::-1]

    return tweets


def thread_header(secao, extra=False):
    """
    Create header for twitter thread with current date, 
    section `secao` and 'EXTRA' if `extra` is True.
    """
    today = datetime.today().strftime('%d/%m/%Y')
    extra_tag = '- EXTRA ' if extra else ''
    description = '(atos normativos)' if secao == 1 else '(alterações de pessoal)'
    pars  = {'data': today, 'secao': secao, 'extra_tag': extra_tag, 'description': description}
    header = 'DOU %(data)s %(extra_tag)s- SEÇÃO %(secao)s %(description)s\n👇 (segue o fio)' % pars
    
    return header


def tags_search(tweet):
    """
    Look for tags representing the section and the 
    edition type in the `tweet` and return a dict
    with the section and the `extra` bool variable. 
    """
    # Guess section from tweet:
    if tweet.find(tag_secao1) != -1 or tweet.find(tag_presidente) != -1:
        secao = 1
    elif tweet.find(tag_secao2) != -1:
        secao = 2
    else:
        secao = '?'

    # Guess edition type from tweet:
    if tweet.find(tag_extra) != -1:
        extra = True
    else:
        extra = False

    return {'secao': secao, 'extra': extra}


def tweet_thread(tweets, tweet_id=None, make_thread=False):
    """
    If `make_thread` is True:
    
    Post a list of tweets (str) `tweets` as a thread on twitter.
    If a `tweet_id` is provided, continue a thread from that
    tweet. If there are more than one tweet in `tweets` and 
    not following a previous tweet, start with a header.
    
    Else:
    
    Post tweets as individual tweets.
    """

    # Prepare tweepy API:
    print('Preparing tweepy...')
    oauth = load_credentials(credentials_file)
    api = tweepy.API(oauth)
    
    # Loop over tweets:
    for tweet in tweets:
        
        if make_thread:       
            # In case we are not following a previous tweet:
            if tweet_id == None:
                if len(tweets) > 1:
                    # Start thread with header:
                    header   = thread_header(**tags_search(tweet))
                    response = api.update_status(header)
                    # Post tweet:
                    response = api.update_status(tweet, response.id)
                else:
                    # Tweet a single tweet:
                    response = api.update_status(tweet)

            # When following previous tweets:
            else:
                response = api.update_status(tweet, tweet_id)

            # Get last tweet's id:
            tweet_id = response.id
        
        else:
            response = api.update_status(tweet)
        
    return response


def load_todays_post_file(secao, template=post_file_template):
    """
    Read a whatsapp post from a file.
    """
    # Prepare filename:
    today = datetime.today().strftime('%Y-%m-%d')
    filename = template % {'secao': secao, 'data': today}
    
    # Read post from file:
    with open(filename, 'r') as f:
        post = f.read()
       
    # Remove spare emojis from the end of the message:
    post = re.sub(r'\n\n.*?$', '', post)
    
    return post


def main():
    
    # Load and post section 2's articles:
    print("Loading today's post for section 2...")
    full_message = load_todays_post_file(2)
    print("Transforming to tweets...")
    tweets = full_message_to_tweet_list(full_message)
    print("Posting on twitter...")
    response = tweet_thread(tweets)

    # Load and post section 1's articles:
    print("Loading today's post for section 1...")
    full_message = load_todays_post_file(1)
    print("Transforming to tweets...")
    tweets = full_message_to_tweet_list(full_message)
    print("Posting on twitter...")
    response = tweet_thread(tweets)
    

#####################
### Run as script ###
#####################

print("Will run as script.")
if __name__=="__main__":
    print("Will call main.")
    main()
