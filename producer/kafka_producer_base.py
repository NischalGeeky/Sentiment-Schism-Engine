import json
from kafka import KafkaProducer


def make_producer():
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def classify_ecosystem(subreddit: str, score: int = 0, num_comments: int = 0) -> str:
    """Assign a post to an ecosystem using the project's existing subreddit rules."""
    enthusiast_subs = {"apple", "gadgets", "hardware", "homelab", "buildapc"}
    critic_subs = {"technology", "privacy", "anticonsumption", "android"}
    subreddit = (subreddit or "").lower()

    if subreddit in enthusiast_subs:
        return "enthusiasts"
    if subreddit in critic_subs:
        return "critics"
    return "public"


def build_message(
    post: dict,
    source: str,
    analysis_topic: str,
    matched_keywords: list[str] | None = None,
) -> dict:
    subreddit = post.get("subreddit", "")
    ecosystem = classify_ecosystem(
        subreddit,
        post.get("score", 0),
        post.get("num_comments", 0),
    )
    title = post.get("title", "") or ""
    body = post.get("selftext", "") or title

    return {
        "post_id": post.get("id", ""),
        "title": title,
        "text": body,
        "author": post.get("author", ""),
        "subreddit": subreddit,
        "score": post.get("score", 0),
        "num_comments": post.get("num_comments", 0),
        "created_utc": post.get("created_utc", 0),
        "ecosystem": ecosystem,
        "source": source,
        "analysis_topic": analysis_topic,
        "matched_keywords": matched_keywords or [],
    }


TOPIC_MAP = {
    "enthusiasts": "reddit.enthusiasts",
    "critics": "reddit.critics",
    "public": "reddit.public",
}
