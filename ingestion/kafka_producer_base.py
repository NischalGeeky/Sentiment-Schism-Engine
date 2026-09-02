import json
from kafka import KafkaProducer

def make_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def classify_ecosystem(subreddit: str, score: int, num_comments: int) -> str:
    enthusiast_subs = {'apple', 'hardware', 'homelab', 'buildapc'}
    critic_subs = {'technology', 'privacy', 'anticonsumption', 'android'}
    
    if subreddit.lower() in enthusiast_subs:
        return 'enthusiasts'
    elif subreddit.lower() in critic_subs:
        return 'critics'
    else:
        return 'public'

def build_message(post: dict, source: str) -> dict:
    subreddit = post.get('subreddit', '')
    score = post.get('score', 0)
    num_comments = post.get('num_comments', 0)
    ecosystem = classify_ecosystem(subreddit, score, num_comments)
    
    return {
        'post_id':      post.get('id', ''),
        'title':        post.get('title', ''),
        'text':         post.get('selftext', '') or post.get('title', ''),
        'author':       post.get('author', ''),
        'subreddit':    subreddit,
        'score':        score,
        'num_comments': num_comments,
        'created_utc':  post.get('created_utc', 0),
        'ecosystem':    ecosystem,
        'source':       source,   # 'arctic' or 'live'
    }

TOPIC_MAP = {
    'enthusiasts': 'reddit.enthusiasts',
    'critics':     'reddit.critics',
    'public':      'reddit.public',
}
