"""Filter an Arctic Shift JSONL dump to one configured subject, then publish to Kafka."""
import argparse
import json
import re
from pathlib import Path

from kafka_producer_base import make_producer, build_message, TOPIC_MAP


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dump_path", help="Path to the Arctic Shift JSONL dump")
    parser.add_argument(
        "--topic-config",
        default=str(Path(__file__).resolve().parents[1] / "config" / "topic_config.json"),
        help="JSON file containing topic_name and keywords",
    )
    parser.add_argument("--flush-every", type=int, default=1000)
    return parser.parse_args()


def stream_dump(path: str):
    """Yield one JSON object per non-empty JSONL line."""
    with open(path, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"Skipping malformed JSON at line {line_number}: {exc}")


def load_topic_config(path: str):
    with open(path, "r", encoding="utf-8") as file:
        config = json.load(file)

    topic_name = str(config.get("topic_name", "")).strip()
    keywords = [str(item).strip() for item in config.get("keywords", []) if str(item).strip()]
    if not topic_name:
        raise ValueError("topic_config.json must contain a non-empty 'topic_name'.")
    if not keywords:
        raise ValueError("topic_config.json must contain at least one keyword or phrase.")

    # Phrase-aware, case-insensitive matching. Word boundaries avoid matching
    # short terms as arbitrary substrings inside unrelated words.
    patterns = [
        (keyword, re.compile(r"(?<!\w)" + re.escape(keyword) + r"(?!\w)", re.IGNORECASE))
        for keyword in keywords
    ]
    return topic_name, patterns


def find_matches(title: str, body: str, patterns):
    searchable_text = f"{title or ''} {body or ''}"
    return [keyword for keyword, pattern in patterns if pattern.search(searchable_text)]


def run():
    args = parse_args()
    topic_name, patterns = load_topic_config(args.topic_config)
    producer = make_producer()

    scanned = published = skipped_no_title = skipped_no_match = 0
    print(f"Input: {Path(args.dump_path)}")
    print(f"Analysis topic: {topic_name}")
    print(f"Keywords: {[keyword for keyword, _ in patterns]}")

    try:
        for raw_post in stream_dump(args.dump_path):
            scanned += 1
            title = raw_post.get("title", "") or ""
            body = raw_post.get("selftext", "") or ""
            if body == "[removed]":
                body = ""
            if not title.strip():
                skipped_no_title += 1
                continue

            # Use title + body for filtering, even if the body is missing/removed.
            matches = find_matches(title, body, patterns)
            if not matches:
                skipped_no_match += 1
                continue

            # Preserve the original fallback: title becomes text when body is unavailable.
            raw_post["selftext"] = body or title
            message = build_message(
                raw_post,
                source="arctic",
                analysis_topic=topic_name,
                matched_keywords=matches,
            )
            producer.send(TOPIC_MAP[message["ecosystem"]], message)
            published += 1

            if published % args.flush_every == 0:
                producer.flush()
                print(
                    f"Scanned {scanned:,} | Published {published:,} | "
                    f"No title {skipped_no_title:,} | No topic match {skipped_no_match:,}"
                )

        producer.flush()
    finally:
        producer.close()

    print("\nIngestion complete")
    print(f"Scanned: {scanned:,}")
    print(f"Published: {published:,}")
    print(f"Skipped (missing title): {skipped_no_title:,}")
    print(f"Skipped (no topic match): {skipped_no_match:,}")


if __name__ == "__main__":
    run()
