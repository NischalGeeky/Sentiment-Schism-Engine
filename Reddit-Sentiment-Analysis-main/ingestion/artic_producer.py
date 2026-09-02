import json, time, sys
from kafka_producer_base import make_producer, build_message, TOPIC_MAP

DUMP_PATH = sys.argv[1]
BATCH_DELAY = 0.001  # 1ms, plain JSONL is fast

def stream_dump(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def run():
    producer = make_producer()
    count = 0
    skipped = 0
    print(f"Loading: {DUMP_PATH}")

    for raw_post in stream_dump(DUMP_PATH):
        # skip removed posts with no title fallback
        text = raw_post.get('selftext', '')
        title = raw_post.get('title', '')
        if text == '[removed]' or not text:
            raw_post['selftext'] = title  # fall back to title

        if not title:
            skipped += 1
            continue

        msg = build_message(raw_post, source='arctic')
        topic = TOPIC_MAP[msg['ecosystem']]
        producer.send(topic, msg)
        count += 1

        if count % 1000 == 0:
            producer.flush()
            print(f"Sent {count} | Skipped {skipped}")

    producer.flush()
    print(f"Done. Sent: {count} | Skipped: {skipped}")

if __name__ == '__main__':
    run()
