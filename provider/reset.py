# reset_redis.py
from redis_client import create_redis_clients
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reset_redis_data(site_name="imovelweb"):
    """Completely reset Redis data for a fresh start."""
    clients = create_redis_clients(
        site_name=site_name,
        host="5.161.248.214",
        port=6379,
        password="redispass"
    )
    
    keys_to_delete = [
        f"scrape_session_{site_name}",
        f"processed_urls_{site_name}",
        f"urls_stream_{site_name}", 
        f"db_sync_{site_name}",
    ]
    
    for key in keys_to_delete:
        try:
            clients['processed_urls'].client.delete(key)
            logger.info(f"✅ Deleted: {key}")
        except Exception as e:
            logger.error(f"Failed to delete {key}: {e}")
    
    # Recreate stream consumer group
    clients['url_stream'].create_consumer_group()
    
    logger.info("🎉 Redis reset complete! Ready for fresh start.")

if __name__ == "__main__":
    reset_redis_data("imovelweb")