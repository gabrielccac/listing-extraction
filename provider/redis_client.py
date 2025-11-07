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

class ScrapeSessionClient(RedisClient):
    """
    Manages scrape session data for intra-session tracking and expired detection.
    """
    
    def __init__(self, site_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.site_name = site_name
        self.scrape_session_key = f"scrape_session_{site_name}"
    
    def update_url_price(self, url: str, price: int):
        """Update URL price in current scrape session (overwrites existing)."""
        self.client.hset(self.scrape_session_key, url, price)
    
    def get_url_price(self, url: str) -> Optional[int]:
        """Get URL price from current scrape session."""
        price = self.client.hget(self.scrape_session_key, url)
        return int(price) if price else None
    
    def get_all_urls(self) -> List[str]:
        """Get all URLs from current scrape session."""
        return list(self.client.hkeys(self.scrape_session_key))
    
    def clear_session(self):
        """Clear current scrape session (call at start of new session)."""
        self.client.delete(self.scrape_session_key)
        logger.info("🧹 Cleared scrape session")

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
    
    def update_url_price(self, url: str, price: int, metadata: Dict = None):
        """Update URL price in processed store (preserves existing data)."""
        existing_data = self.get_url_data(url) or {}
        
        updated_data = {
            **existing_data,
            'price': price,
            'last_seen': time.time(),
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

class DbSyncClient(RedisClient):
    """
    Manages database sync state (mirrors Airtable for fast comparisons).
    """
    
    def __init__(self, site_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.site_name = site_name
        self.db_sync_key = f"db_sync_{site_name}"
    
    def sync_from_processed(self, processed_client: ProcessedUrlsClient):
        """Full sync from processed_urls to db_sync."""
        all_data = {}
        for url in processed_client.get_all_urls():
            data = processed_client.get_url_data(url)
            if data:
                all_data[url] = json.dumps(data)
        
        if all_data:
            self.client.hset(self.db_sync_key, mapping=all_data)
            logger.info(f"🔄 Full sync: {len(all_data)} URLs to db_sync")
    
    def get_all_urls(self) -> List[str]:
        """Get all URLs from db_sync."""
        return list(self.client.hkeys(self.db_sync_key))
    
    def add_url(self, url: str, data: Dict):
        """Add URL to db_sync."""
        self.client.hset(self.db_sync_key, url, json.dumps(data))
    
    def remove_url(self, url: str):
        """Remove URL from db_sync."""
        self.client.hdel(self.db_sync_key, url)

# Factory functions for easy creation
def create_redis_clients(site_name: str, host: str, port: int, password: str):
    """Create all Redis clients for a site."""
    clients = {
        'scrape_session': ScrapeSessionClient(site_name, host, port, password, db=0),
        'processed_urls': ProcessedUrlsClient(site_name, host, port, password, db=0),
        'url_stream': UrlStreamClient(site_name, host, port, password, db=0),
        'db_sync': DbSyncClient(site_name, host, port, password, db=1)  # Separate DB
    }
    
    # Connect all clients
    for name, client in clients.items():
        client.connect()
    
    # Ensure stream consumer group exists
    clients['url_stream'].create_consumer_group()
    
    return clients