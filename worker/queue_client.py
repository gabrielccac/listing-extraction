"""
Queue Client - RabbitMQ + Redis Manager

Direct copy from utils/queue_manager.py for modularization.
Thread-safe Redis, per-worker RabbitMQ connections.
"""

import pika
import redis
import hashlib
import logging
from typing import List, Dict, Optional, Callable

logger = logging.getLogger(__name__)


class RedisDeduplicator:
    """
    Thread-safe Redis client for URL deduplication.
    Uses Redis sets to track already-scraped URLs and prevent duplicates.
    Safe to share across multiple worker threads.
    """

    def __init__(self, host: str, port: int, password: str, set_name: str):
        """
        Initialize Redis client and test connection.

        Args:
            host: Redis server hostname/IP
            port: Redis server port (default 6379)
            password: Redis authentication password
            set_name: Redis set name for storing URLs (e.g., 'scraped_urls_olx')

        Raises:
            redis.ConnectionError: If unable to connect to Redis
        """
        self.host = host
        self.port = port
        self.set_name = set_name

        try:
            self.client = redis.Redis(
                host=host,
                port=port,
                password=password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            self.client.ping()
            logger.info(f"✅ Connected to Redis at {host}:{port} (set: {set_name})")
        except redis.ConnectionError as e:
            logger.error(f"❌ Failed to connect to Redis at {host}:{port}: {e}")
            raise

    def filter_new_urls(self, urls: List[str]) -> List[str]:
        """Batch check URLs against Redis set and return only new ones."""
        if not urls:
            return []

        try:
            existing_flags = self.client.smismember(self.set_name, urls)
            new_urls = [url for url, exists in zip(urls, existing_flags) if not exists]

            duplicate_count = len(urls) - len(new_urls)
            if duplicate_count > 0:
                logger.debug(f"🔍 Filtered {len(new_urls)} new URLs, {duplicate_count} duplicates")

            return new_urls
        except Exception as e:
            logger.error(f"❌ Error filtering URLs in Redis: {e}")
            return urls

    def add_urls(self, urls: List[str]) -> int:
        """Batch add URLs to Redis set."""
        if not urls:
            return 0

        try:
            added_count = self.client.sadd(self.set_name, *urls)
            logger.debug(f"✅ Added {added_count} URLs to Redis set '{self.set_name}'")
            return added_count
        except Exception as e:
            logger.error(f"❌ Error adding URLs to Redis: {e}")
            return 0

    def url_exists(self, url: str) -> bool:
        """Check if a single URL exists in Redis set."""
        try:
            return self.client.sismember(self.set_name, url)
        except Exception as e:
            logger.error(f"❌ Error checking URL in Redis: {e}")
            return False

    def get_total_count(self) -> int:
        """Get total number of URLs in Redis set."""
        try:
            return self.client.scard(self.set_name)
        except Exception as e:
            logger.error(f"❌ Error getting count from Redis: {e}")
            return 0

    def close(self):
        """Close Redis connection (optional, connections are auto-managed)"""
        try:
            self.client.close()
            logger.info(f"🧹 Redis connection closed")
        except Exception as e:
            logger.warning(f"⚠️ Error closing Redis connection: {e}")


class QueueManager:
    """
    RabbitMQ manager for publishing and consuming URLs.
    Each worker should create its own QueueManager instance (not thread-safe).
    Handles message publishing, consuming, and retry logic.
    """

    def __init__(self, host: str, user: str, password: str, queue_name: str):
        """Initialize RabbitMQ manager (does not connect yet)."""
        self.host = host
        self.user = user
        self.password = password
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        """Create connection and channel, declare queue as durable."""
        try:
            credentials = pika.PlainCredentials(self.user, self.password)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            self.channel = self.connection.channel()

            # Declare queue as durable (survives RabbitMQ restarts)
            self.channel.queue_declare(queue=self.queue_name, durable=True)

            logger.info(f"✅ Connected to RabbitMQ at {self.host} (queue: {self.queue_name})")
        except Exception as e:
            logger.error(f"❌ Failed to connect to RabbitMQ at {self.host}: {e}")
            raise

    def publish_urls(self, urls: List[str], metadata: Optional[Dict] = None):
        """Batch publish URLs to RabbitMQ queue with persistence."""
        if not urls:
            return

        if not self.channel:
            logger.error("❌ RabbitMQ channel not initialized. Call connect() first.")
            return

        try:
            for url in urls:
                url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
                headers = metadata.copy() if metadata else {}
                headers['retry_count'] = 0

                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=url.encode('utf-8'),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                        message_id=url_hash,
                        headers=headers
                    )
                )

            logger.debug(f"✅ Published {len(urls)} URLs to queue '{self.queue_name}'")
        except Exception as e:
            logger.error(f"❌ Error publishing URLs to RabbitMQ: {e}")

    def publish_single_url(self, url: str, metadata: Optional[Dict] = None):
        """Publish a single URL to RabbitMQ queue."""
        self.publish_urls([url], metadata)

    def requeue_with_retry(self, ch, method, properties, body, max_retries: int = 3) -> bool:
        """Requeue a message with incremented retry count."""
        try:
            headers = properties.headers or {}
            retry_count = headers.get('retry_count', 0)

            if retry_count >= max_retries:
                logger.warning(f"⚠️ Max retries ({max_retries}) exceeded for URL: {body.decode('utf-8')[:100]}")
                return False

            new_retry_count = retry_count + 1
            headers['retry_count'] = new_retry_count

            ch.basic_publish(
                exchange='',
                routing_key=method.routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                    message_id=properties.message_id,
                    headers=headers
                )
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

            logger.info(f"🔄 Requeued URL (attempt {new_retry_count}/{max_retries}): {body.decode('utf-8')[:100]}")
            return True

        except Exception as e:
            logger.error(f"❌ Error requeuing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return False

    def get_retry_count(self, properties) -> int:
        """Extract retry count from message properties."""
        if properties.headers:
            return properties.headers.get('retry_count', 0)
        return 0

    def consume(self, callback: Callable, prefetch_count: int = 1):
        """Start consuming messages from queue."""
        if not self.channel:
            logger.error("❌ RabbitMQ channel not initialized. Call connect() first.")
            return

        try:
            self.channel.basic_qos(prefetch_count=prefetch_count)

            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )

            logger.info(f"🎧 Started consuming from queue '{self.queue_name}'")
            self.channel.start_consuming()

        except KeyboardInterrupt:
            logger.info("⚠️ Consumer interrupted by user")
            self.stop_consuming()
        except Exception as e:
            logger.error(f"❌ Error consuming from queue: {e}")
            raise

    def stop_consuming(self):
        """Stop consuming messages gracefully."""
        if self.channel:
            try:
                self.channel.stop_consuming()
                logger.info("🛑 Stopped consuming messages")
            except Exception as e:
                logger.warning(f"⚠️ Error stopping consumer: {e}")

    def get_queue_size(self) -> int:
        """Get current number of messages in queue."""
        if not self.channel:
            logger.error("❌ RabbitMQ channel not initialized")
            return 0

        try:
            queue_info = self.channel.queue_declare(
                queue=self.queue_name,
                durable=True,
                passive=True
            )
            return queue_info.method.message_count
        except Exception as e:
            logger.error(f"❌ Error getting queue size: {e}")
            return 0

    def purge_queue(self):
        """Delete all messages from queue (use with caution!)."""
        if not self.channel:
            logger.error("❌ RabbitMQ channel not initialized")
            return 0

        try:
            result = self.channel.queue_purge(queue=self.queue_name)
            logger.warning(f"⚠️ Purged {result.method.message_count} messages from queue '{self.queue_name}'")
            return result.method.message_count
        except Exception as e:
            logger.error(f"❌ Error purging queue: {e}")
            return 0

    def close(self):
        """Close RabbitMQ connection gracefully."""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info(f"🧹 RabbitMQ connection closed")
        except Exception as e:
            logger.warning(f"⚠️ Error closing RabbitMQ connection: {e}")


# --- FACTORY FUNCTIONS ---

def create_redis_deduplicator(site_name: str) -> RedisDeduplicator:
    """Factory function to create RedisDeduplicator with standard config."""
    return RedisDeduplicator(
        host='5.161.248.214',
        port=6379,
        password='redispass',
        set_name=f'processed_urls_{site_name}'  # Changed to match consumer convention
    )


def create_queue_manager(site_name: str) -> QueueManager:
    """Factory function to create QueueManager with standard config."""
    return QueueManager(
        host='5.161.248.214',
        user='admin',
        password='rabbitmqpass',
        queue_name=f'scraped_urls_{site_name}'  # Changed to match consumer convention
    )