import pika
from urllib.parse import urlparse, urlunparse

# === CONFIG ===
RABBITMQ_HOST = '5.161.248.214'
RABBITMQ_USER = 'admin'
RABBITMQ_PASSWORD = 'rabbitmqpass'
QUEUE_NAME = 'scraped_urls_imovelweb'

print("Starting URL fixer: removing double slashes in path (// → /)")
print(f"Queue: {QUEUE_NAME} | Processing all ~40,000+ messages...\n")

# === CONNECTION ===
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
)
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# Speed: fetch 500 messages at a time
channel.basic_qos(prefetch_count=500)

def fix_url(url):
    """Fix // in path: https://domain.com//path → https://domain.com/path"""
    parsed = urlparse(url)
    # Replace ALL occurrences of // in the path with single /
    fixed_path = parsed.path.replace('//', '/')
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        fixed_path,
        parsed.params,
        parsed.query,
        parsed.fragment
    ))

fixed_count = 0
unchanged_count = 0

def callback(ch, method, properties, body):
    global fixed_count, unchanged_count
    url = body.decode('utf-8').strip()
    fixed_url = fix_url(url)

    if fixed_url != url:
        # Requeue fixed version
        ch.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=fixed_url.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)  # persistent
        )
        print(f"FIXED → {fixed_url}")
        fixed_count += 1
    else:
        # Optionally requeue unchanged ones too (recommended)
        ch.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=url.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        unchanged_count += 1

    # Always ack the original
    ch.basic_ack(delivery_tag=method.delivery_tag)

# === START CONSUMING ===
print("Consuming... (this will process ALL messages, even if queue looks empty due to unacked ones)")
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("\n\nStopped by user.")
finally:
    print(f"\nFINISHED!")
    print(f"Fixed URLs    : {fixed_count}")
    print(f"Unchanged URLs: {unchanged_count}")
    print(f"Total processed: {fixed_count + unchanged_count}")
    print(f"All clean URLs are now back in: {QUEUE_NAME}")
    connection.close()