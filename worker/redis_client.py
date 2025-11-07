import redis
import time
import json
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class RedisClient:
    """
    Core Redis client with connection management and basic operations.
    """
    
    def __init__(self, host: str, port: int, password: str, db: int = 0):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.client = None
        
    def connect(self):
        """Establish Redis connection."""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10
            )
            self.client.ping()
            logger.info(f"✅ Redis connected to db{self.db}")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise
    
    def close(self):
        """Close Redis connection."""
        if self.client:
            self.client.close()
            logger.debug("Redis connection closed")

class ProcessedUrlsClient(RedisClient):
    """
    Manages processed URLs with full JSON data for deduplication and worker results.
    """
    
    def __init__(self, site_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.site_name = site_name
        self.processed_key = f"processed_urls_{site_name}"
    
    def get_url_data(self, url: str) -> Optional[Dict]:
        """Get full URL data from processed store."""
        data_json = self.client.hget(self.processed_key, url)
        if data_json:
            return json.loads(data_json)
        return None

    def get_urls_batch(self, urls: List[str]) -> Dict[str, Dict]:
        """
        Get multiple URLs data in a single Redis operation (HMGET).

        Returns: Dict mapping URL -> data (only includes URLs that exist)
        """
        if not urls:
            return {}

        # Use HMGET for batch retrieval
        results = self.client.hmget(self.processed_key, urls)

        # Build dict of existing URLs only
        existing_data = {}
        for url, data_json in zip(urls, results):
            if data_json:
                existing_data[url] = json.loads(data_json)

        return existing_data
    
    def update_url_price(self, url: str, price: int, metadata: Dict = None):
        """Update URL price in processed store (preserves existing data)."""
        existing_data = self.get_url_data(url) or {}

        updated_data = {
            **existing_data,
            'price': price,
            **(metadata or {})
        }

        # Ensure first_seen is preserved
        if 'first_seen' not in updated_data:
            updated_data['first_seen'] = time.time()

        self.client.hset(self.processed_key, url, json.dumps(updated_data))
    
    def update_full_data(self, url: str, property_data: Dict):
        """Update with full property data from worker processing."""
        existing_data = self.get_url_data(url) or {}

        # Merge existing metadata with new property data
        merged_data = {
            **existing_data,  # Keep price, timestamps, metadata
            **property_data,  # Override with fresh property data
            'last_processed': time.time()
        }

        # Ensure first_seen is preserved (or set for new URLs)
        if 'first_seen' not in merged_data:
            merged_data['first_seen'] = time.time()

        self.client.hset(self.processed_key, url, json.dumps(merged_data))
    
    def get_all_urls(self) -> List[str]:
        """Get all URLs from processed store."""
        return list(self.client.hkeys(self.processed_key))
    
    def remove_url(self, url: str):
        """Remove URL from processed store (for expired listings)."""
        self.client.hdel(self.processed_key, url)
        logger.debug(f"Removed URL from processed: {url[:80]}")

class UrlStreamClient(RedisClient):
    """
    Manages Redis Stream for URL processing queue (replaces RabbitMQ).
    """
    
    def __init__(self, site_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.site_name = site_name
        self.stream_key = f"urls_stream_{site_name}"
        self.consumer_group = f"workers_{site_name}"
    
    def publish_url(self, url: str, action: str = "process"):
        """Publish URL to stream for worker processing."""
        message = {
            'url': url,
            'action': action,
            'timestamp': str(time.time())
        }
        message_id = self.client.xadd(self.stream_key, message)
        logger.debug(f"📤 Published to stream: {url[:80]} ({action})")
        return message_id

    def publish_urls_batch(self, url_action_pairs: List[tuple]):
        """
        Publish multiple URLs to stream in a single pipeline operation.

        Args:
            url_action_pairs: List of (url, action) tuples

        Returns: List of message IDs
        """
        if not url_action_pairs:
            return []

        timestamp = str(time.time())

        # Use pipeline for batch publishing
        with self.client.pipeline() as pipe:
            for url, action in url_action_pairs:
                message = {
                    'url': url,
                    'action': action,
                    'timestamp': timestamp
                }
                pipe.xadd(self.stream_key, message)

            message_ids = pipe.execute()

        logger.debug(f"📤 Published {len(url_action_pairs)} URLs to stream in batch")
        return message_ids
    
    def create_consumer_group(self):
        """Create consumer group for workers (idempotent)."""
        try:
            self.client.xgroup_create(
                self.stream_key, 
                self.consumer_group, 
                id='0',  # Start from beginning
                mkstream=True  # Create stream if doesn't exist
            )
            logger.info(f"✅ Created consumer group: {self.consumer_group}")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group already exists: {self.consumer_group}")
            else:
                raise
    
    def consume_urls(self, consumer_name: str, count: int = 1, block_ms: int = 5000):
        """
        Consume URLs from stream (blocking).
        
        Returns: List of (message_id, url, fields)
        """
        try:
            messages = self.client.xreadgroup(
                groupname=self.consumer_group,
                consumername=consumer_name,
                streams={self.stream_key: '>'},  # '>' means unread messages
                count=count,
                block=block_ms
            )
            
            results = []
            for stream, message_list in messages:
                for message_id, fields in message_list:
                    results.append((message_id, fields['url'], fields))
            
            return results
            
        except Exception as e:
            logger.error(f"Stream consumption error: {e}")
            return []
    
    def ack_message(self, message_id: str):
        """Acknowledge message processing."""
        self.client.xack(self.stream_key, self.consumer_group, message_id)
    
    def get_pending_count(self) -> int:
        """Get number of pending messages."""
        pending_info = self.client.xpending(self.stream_key, self.consumer_group)
        return pending_info['pending'] if pending_info else 0

class FailedUrlsClient(RedisClient):
    """
    Manages failed URLs that couldn't be processed after max retries.
    Stores URL → error message for debugging.
    """

    def __init__(self, site_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.site_name = site_name
        self.failed_key = f"failed_urls_{site_name}"

    def add_failed_url(self, url: str, error_message: str):
        """Store a failed URL with error message."""
        self.client.hset(self.failed_key, url, error_message)
        logger.debug(f"❌ Stored failed URL: {url[:80]}")

    def get_error(self, url: str) -> Optional[str]:
        """Get error message for a failed URL."""
        return self.client.hget(self.failed_key, url)

    def remove_url(self, url: str):
        """Remove URL from failed list (after successful retry)."""
        self.client.hdel(self.failed_key, url)
        logger.debug(f"✅ Removed from failed URLs: {url[:80]}")

    def get_all_failed_urls(self) -> List[str]:
        """Get all failed URLs."""
        return list(self.client.hkeys(self.failed_key))

    def get_failed_count(self) -> int:
        """Get count of failed URLs."""
        return self.client.hlen(self.failed_key)

    def clear_all(self):
        """Clear all failed URLs (use after batch retry)."""
        count = self.get_failed_count()
        self.client.delete(self.failed_key)
        logger.info(f"🧹 Cleared {count} failed URLs")

# Factory function for worker
def create_redis_clients(site_name: str, host: str, port: int, password: str):
    """Create Redis clients needed for worker."""
    clients = {
        'processed_urls': ProcessedUrlsClient(site_name, host, port, password, db=0),
        'url_stream': UrlStreamClient(site_name, host, port, password, db=0),
        'failed_urls': FailedUrlsClient(site_name, host, port, password, db=0)
    }

    # Connect all clients
    for name, client in clients.items():
        client.connect()

    # Ensure stream consumer group exists
    clients['url_stream'].create_consumer_group()

    return clients