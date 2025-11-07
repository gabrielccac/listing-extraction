#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Bulk Publisher - Enqueue URLs from Redis to RabbitMQ

Usage:
    python bulk_publish.py --site imovelweb
    python bulk_publish.py --site olx
"""
import argparse
import redis
import pika
import json
import logging
from typing import List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
REDIS_HOST = '5.161.248.214'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redispass'

RABBITMQ_HOST = '5.161.248.214'
RABBITMQ_USER = 'admin'
RABBITMQ_PASSWORD = 'rabbitmqpass'


def bulk_publish(site_name: str, batch_size: int = 1000):
    """
    Publish all URLs from Redis hash to RabbitMQ queue.
    
    Args:
        site_name: Website identifier (e.g., 'imovelweb', 'olx')
        batch_size: Number of URLs to publish per batch
    """
    session_hash = f'scrape_session_{site_name}'
    queue_name = f'scraped_urls_{site_name}'
    
    print(f"\n{'='*70}")
    print(f" BULK PUBLISHER - {site_name.upper()}")
    print(f"{'='*70}")
    print(f" Source:      Redis hash '{session_hash}'")
    print(f" Destination: RabbitMQ queue '{queue_name}'")
    print(f"{'='*70}\n")
    
    # Connect to Redis
    logger.info("Connecting to Redis...")
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        r.ping()
        logger.info("✓ Redis connected")
    except Exception as e:
        logger.error(f"✗ Redis connection failed: {e}")
        return
    
    # Check if hash exists and get count
    total_urls = r.hlen(session_hash)
    if total_urls == 0:
        logger.warning(f"✗ Hash '{session_hash}' is empty or doesn't exist")
        return
    
    logger.info(f"✓ Found {total_urls:,} URLs in Redis")
    
    # Connect to RabbitMQ
    logger.info("Connecting to RabbitMQ...")
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
        )
        channel = connection.channel()
        
        # Declare queue as durable
        channel.queue_declare(queue=queue_name, durable=True)
        
        logger.info("✓ RabbitMQ connected")
    except Exception as e:
        logger.error(f"✗ RabbitMQ connection failed: {e}")
        return
    
    # Publish URLs in batches
    logger.info(f"\n📤 Publishing {total_urls:,} URLs to queue...")
    
    cursor = 0
    published = 0
    batch = []
    
    try:
        while True:
            # Scan batch from Redis hash
            cursor, items = r.hscan(session_hash, cursor, count=batch_size)
            
            for url, data_json in items.items():
                batch.append(url)
                
                # Publish batch when full
                if len(batch) >= batch_size:
                    _publish_batch(channel, queue_name, batch)
                    published += len(batch)
                    logger.info(f"  Published {published:,}/{total_urls:,} URLs ({published/total_urls*100:.1f}%)")
                    batch = []
            
            # Break if we've scanned everything
            if cursor == 0:
                break
        
        # Publish remaining URLs
        if batch:
            _publish_batch(channel, queue_name, batch)
            published += len(batch)
            logger.info(f"  Published {published:,}/{total_urls:,} URLs (100.0%)")
        
        connection.close()
        
        print(f"\n{'='*70}")
        print(f" ✅ PUBLISH COMPLETE")
        print(f"{'='*70}")
        print(f"  Total URLs published: {published:,}")
        print(f"  Queue: {queue_name}")
        print(f"{'='*70}\n")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interrupted by user")
        connection.close()
    except Exception as e:
        logger.error(f"\n✗ Error during publish: {e}")
        connection.close()


def _publish_batch(channel, queue_name: str, urls: List[str]):
    """
    Publish a batch of URLs to RabbitMQ.
    
    Args:
        channel: Pika channel
        queue_name: Queue name
        urls: List of URLs to publish
    """
    for url in urls:
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=url.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE  # Persistent
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Bulk publish URLs from Redis to RabbitMQ')
    parser.add_argument('--site', required=True, help='Site name (e.g., imovelweb, olx)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size (default: 1000)')
    
    args = parser.parse_args()
    
    bulk_publish(args.site, args.batch_size)