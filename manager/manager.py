#!/usr/bin/env python3
"""
Manager - Syncs Redis processed_urls with Airtable

Responsibilities:
1. Find EXPIRED urls: processed_urls - scrape_session → Queue delete tasks
2. Find NEW urls: processed_urls without airtable_record_id → Queue add tasks (backup)
3. Queue tasks to unified airtable_tasks stream

Usage:
    python manager.py --site imovelweb
"""

import argparse
import logging
import sys
import os

# Add parent dir to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'provider'))

from redis_client import (
    ScrapeSessionClient,
    ProcessedUrlsClient,
    AirtableTasksClient
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Redis connection config
REDIS_HOST = '5.161.248.214'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redispass'


class Manager:
    """
    Manager for syncing processed_urls with Airtable.
    Detects expired and new URLs and queues sync tasks.
    """

    def __init__(self, site_name: str):
        self.site_name = site_name

        logger.info(f"Initializing Manager for site: {site_name}")

        # Create Redis clients
        self.scrape_session = ScrapeSessionClient(
            site_name, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, db=0
        )
        self.processed_urls = ProcessedUrlsClient(
            site_name, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, db=0
        )
        self.airtable_tasks = AirtableTasksClient(
            REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, db=0
        )

        # Connect all clients
        self.scrape_session.connect()
        self.processed_urls.connect()
        self.airtable_tasks.connect()

        # Ensure Airtable consumer group exists
        self.airtable_tasks.create_consumer_group()

        logger.info("✅ Manager initialized")

    def find_expired_urls(self):
        """
        Find URLs that are in processed_urls but NOT in current scrape_session.
        These are expired listings that should be deleted from Airtable.

        Returns: Set of expired URLs
        """
        logger.info("🔍 Finding expired URLs...")

        # Get all URLs from both stores
        processed_urls = set(self.processed_urls.get_all_urls())
        session_urls = set(self.scrape_session.get_all_urls())

        # Expired = in processed but not in current session
        expired = processed_urls - session_urls

        logger.info(f"   Processed URLs: {len(processed_urls):,}")
        logger.info(f"   Session URLs:   {len(session_urls):,}")
        logger.info(f"   Expired:        {len(expired):,}")

        return expired

    def find_new_urls(self):
        """
        Find URLs in processed_urls that don't have airtable_record_id yet.
        This is a backup check - normally worker queues 'add' tasks immediately.

        Returns: Set of new URLs
        """
        logger.info("🔍 Finding new URLs without Airtable record_id...")

        all_urls = self.processed_urls.get_all_urls()
        new_urls = set()

        for url in all_urls:
            data = self.processed_urls.get_url_data(url)
            if data and not data.get('airtable_record_id'):
                new_urls.add(url)

        logger.info(f"   URLs without record_id: {len(new_urls):,}")

        return new_urls

    def queue_delete_tasks(self, expired_urls):
        """
        Queue delete tasks for expired URLs.
        Also removes them from processed_urls.
        """
        if not expired_urls:
            logger.info("✅ No expired URLs to delete")
            return

        logger.info(f"📋 Queueing {len(expired_urls):,} delete tasks...")

        for url in expired_urls:
            # Queue Airtable delete task
            self.airtable_tasks.publish_task(
                site=self.site_name,
                action='delete',
                url=url
            )

            # Remove from processed_urls
            self.processed_urls.remove_url(url)

        logger.info(f"✅ Queued {len(expired_urls):,} delete tasks")

    def queue_add_tasks(self, new_urls):
        """
        Queue add tasks for new URLs without airtable_record_id.
        This is a backup - normally worker handles this.
        """
        if not new_urls:
            logger.info("✅ No new URLs to add")
            return

        logger.info(f"📋 Queueing {len(new_urls):,} add tasks (backup)...")

        for url in new_urls:
            self.airtable_tasks.publish_task(
                site=self.site_name,
                action='add',
                url=url
            )

        logger.info(f"✅ Queued {len(new_urls):,} add tasks")

    def run(self):
        """Run the manager sync process."""
        logger.info("=" * 60)
        logger.info(f"MANAGER SYNC - {self.site_name.upper()}")
        logger.info("=" * 60)

        # 1. Find and queue expired URLs
        expired_urls = self.find_expired_urls()
        self.queue_delete_tasks(expired_urls)

        # 2. Find and queue new URLs (backup check)
        new_urls = self.find_new_urls()
        self.queue_add_tasks(new_urls)

        # 3. Show summary
        stream_length = self.airtable_tasks.get_stream_length()

        logger.info("=" * 60)
        logger.info("MANAGER SYNC COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Expired URLs queued for deletion: {len(expired_urls):,}")
        logger.info(f"New URLs queued for addition:     {len(new_urls):,}")
        logger.info(f"Airtable tasks in queue:          {stream_length:,}")
        logger.info("=" * 60)

    def close(self):
        """Close Redis connections."""
        self.scrape_session.close()
        self.processed_urls.close()
        self.airtable_tasks.close()
        logger.info("✅ Redis connections closed")


def main():
    parser = argparse.ArgumentParser(
        description='Manager - Sync processed_urls with Airtable',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python manager.py --site imovelweb
  python manager.py --site dfimoveis
        """
    )

    parser.add_argument(
        '--site',
        required=True,
        help='Site name (e.g., imovelweb, dfimoveis)'
    )

    args = parser.parse_args()

    manager = Manager(args.site)

    try:
        manager.run()
    finally:
        manager.close()


if __name__ == '__main__':
    main()
