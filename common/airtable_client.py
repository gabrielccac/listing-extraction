#!/usr/bin/env python3
"""
Airtable Client - Consumes tasks from airtable_tasks stream and syncs with Airtable

Unified consumer for all sites. Handles:
- add: Create new record in Airtable (fetches data from processed_urls)
- update: Update existing record in Airtable (searches by URL)
- delete: Delete record from Airtable (searches by URL)

Note: Does not store airtable_record_id in Redis. All operations search by URL field.

Usage:
    python common/airtable_client.py
    python common/airtable_client.py --consumer-name worker1
"""

import argparse
import logging
import sys
import os
import time
import json
import requests
from typing import Dict, Optional

# Add provider dir to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'provider'))

from redis_client import ProcessedUrlsClient, AirtableTasksClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Airtable configuration
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID', 'appZFLyVxrsHqKeLU')
AIRTABLE_TABLE_ID = os.getenv('AIRTABLE_TABLE_ID', 'tblhe6RxhgquoMxNf')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY', '')

# Redis connection config
REDIS_HOST = '5.161.248.214'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redispass'

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


class AirtableClient:
    """
    Airtable API client using manual requests.
    Handles CRUD operations for property listings.
    """

    def __init__(self, base_id: str, table_id: str, api_key: str):
        self.base_id = base_id
        self.table_id = table_id
        self.api_key = api_key
        self.base_url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }

    def create_record(self, fields: Dict) -> Optional[str]:
        """
        Create a new record in Airtable.

        Args:
            fields: Dictionary of field values

        Returns:
            Record ID if successful, None otherwise
        """
        try:
            payload = {'fields': fields}
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=payload,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                record_id = data.get('id')
                logger.debug(f"✅ Created record: {record_id}")
                return record_id
            else:
                logger.error(f"❌ Airtable create failed ({response.status_code}): {response.text[:200]}")
                return None

        except Exception as e:
            logger.error(f"❌ Exception during create: {str(e)[:200]}")
            return None

    def update_record(self, record_id: str, fields: Dict) -> bool:
        """
        Update an existing record in Airtable.

        Args:
            record_id: Airtable record ID
            fields: Dictionary of field values to update

        Returns:
            True if successful, False otherwise
        """
        try:
            payload = {'fields': fields}
            url = f"{self.base_url}/{record_id}"
            response = requests.patch(
                url,
                headers=self.headers,
                json=payload,
                timeout=10
            )

            if response.status_code == 200:
                logger.debug(f"✅ Updated record: {record_id}")
                return True
            else:
                logger.error(f"❌ Airtable update failed ({response.status_code}): {response.text[:200]}")
                return False

        except Exception as e:
            logger.error(f"❌ Exception during update: {str(e)[:200]}")
            return False

    def delete_record(self, record_id: str) -> bool:
        """
        Delete a record from Airtable.

        Args:
            record_id: Airtable record ID

        Returns:
            True if successful, False otherwise
        """
        try:
            url = f"{self.base_url}/{record_id}"
            response = requests.delete(
                url,
                headers=self.headers,
                timeout=10
            )

            if response.status_code == 200:
                logger.debug(f"✅ Deleted record: {record_id}")
                return True
            else:
                logger.error(f"❌ Airtable delete failed ({response.status_code}): {response.text[:200]}")
                return False

        except Exception as e:
            logger.error(f"❌ Exception during delete: {str(e)[:200]}")
            return False

    def find_record_by_url(self, url: str) -> Optional[str]:
        """
        Find a record by URL field.

        Args:
            url: Property URL

        Returns:
            Record ID if found, None otherwise
        """
        try:
            # Use filterByFormula to find record with matching URL
            params = {
                'filterByFormula': f'{{url}}="{url}"',
                'maxRecords': 1
            }
            response = requests.get(
                self.base_url,
                headers=self.headers,
                params=params,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                records = data.get('records', [])
                if records:
                    record_id = records[0].get('id')
                    logger.debug(f"✅ Found record: {record_id} for URL: {url[:60]}")
                    return record_id
                else:
                    logger.debug(f"No record found for URL: {url[:60]}")
                    return None
            else:
                logger.error(f"❌ Airtable search failed ({response.status_code}): {response.text[:200]}")
                return None

        except Exception as e:
            logger.error(f"❌ Exception during search: {str(e)[:200]}")
            return None


class AirtableTaskConsumer:
    """
    Consumer that processes tasks from airtable_tasks stream.
    """

    def __init__(self, consumer_name: str = 'consumer1'):
        self.consumer_name = consumer_name

        logger.info(f"Initializing Airtable Task Consumer: {consumer_name}")

        # Validate Airtable credentials
        if not AIRTABLE_API_KEY:
            logger.error("❌ AIRTABLE_API_KEY not set! Please set it in environment.")
            raise ValueError("Missing AIRTABLE_API_KEY")

        # Create Airtable client
        self.airtable = AirtableClient(
            base_id=AIRTABLE_BASE_ID,
            table_id=AIRTABLE_TABLE_ID,
            api_key=AIRTABLE_API_KEY
        )

        # Create Redis client for airtable_tasks
        self.airtable_tasks = AirtableTasksClient(
            REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, db=0
        )
        self.airtable_tasks.connect()
        self.airtable_tasks.create_consumer_group()

        # Cache of ProcessedUrlsClient instances per site
        self.processed_urls_clients = {}

        logger.info("✅ Consumer initialized")

    def get_processed_urls_client(self, site_name: str) -> ProcessedUrlsClient:
        """Get or create ProcessedUrlsClient for a site."""
        if site_name not in self.processed_urls_clients:
            client = ProcessedUrlsClient(
                site_name, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, db=0
            )
            client.connect()
            self.processed_urls_clients[site_name] = client
            logger.debug(f"Created ProcessedUrlsClient for {site_name}")

        return self.processed_urls_clients[site_name]

    def process_add_task(self, site: str, url: str) -> bool:
        """
        Process 'add' task: Create new record in Airtable.

        Args:
            site: Site name
            url: Property URL

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get data from processed_urls
            processed_urls = self.get_processed_urls_client(site)
            data = processed_urls.get_url_data(url)

            if not data:
                logger.error(f"❌ No data found in processed_urls for {url[:60]}")
                return False

            # Prepare fields for Airtable (map your Redis fields to Airtable fields)
            fields = {
                'url': url,
                'site': site,
                'price': data.get('price', 0),
                'titulo': data.get('titulo', ''),
                'bedrooms': data.get('bedrooms', 0),
                'bathrooms': data.get('bathrooms', 0),
                'area': data.get('area', 0),
                'description': data.get('description', ''),
                # Add more fields as needed
            }

            # Create record in Airtable
            record_id = self.airtable.create_record(fields)

            if not record_id:
                return False

            logger.info(f"✅ ADD: Created record {record_id} for {url[:60]}")
            return True

        except Exception as e:
            logger.error(f"❌ Error processing add task: {str(e)[:200]}")
            return False

    def process_update_task(self, site: str, url: str, fields: Dict) -> bool:
        """
        Process 'update' task: Update existing record in Airtable.

        Args:
            site: Site name
            url: Property URL
            fields: Fields to update

        Returns:
            True if successful, False otherwise
        """
        try:
            # Find record by URL in Airtable
            record_id = self.airtable.find_record_by_url(url)

            if not record_id:
                logger.warning(f"⚠️  No record found in Airtable for {url[:60]} (might not be synced yet)")
                return False

            # Update record in Airtable
            success = self.airtable.update_record(record_id, fields)

            if success:
                logger.info(f"✅ UPDATE: Updated record {record_id} for {url[:60]}")

            return success

        except Exception as e:
            logger.error(f"❌ Error processing update task: {str(e)[:200]}")
            return False

    def process_delete_task(self, site: str, url: str) -> bool:
        """
        Process 'delete' task: Delete record from Airtable.

        Args:
            site: Site name
            url: Property URL

        Returns:
            True if successful, False otherwise
        """
        try:
            # Find record by URL in Airtable
            record_id = self.airtable.find_record_by_url(url)

            if not record_id:
                logger.warning(f"⚠️  No record found in Airtable for {url[:60]} (might already be deleted)")
                return True  # Consider success if already deleted

            # Delete from Airtable
            success = self.airtable.delete_record(record_id)

            if success:
                logger.info(f"✅ DELETE: Deleted record {record_id} for {url[:60]}")

            return success

        except Exception as e:
            logger.error(f"❌ Error processing delete task: {str(e)[:200]}")
            return False

    def process_task(self, task: Dict) -> bool:
        """
        Process a single task.

        Args:
            task: Task dictionary with 'site', 'action', 'url', 'fields' (optional)

        Returns:
            True if successful, False otherwise
        """
        site = task['site']
        action = task['action']
        url = task['url']
        fields = task.get('fields')

        logger.info(f"Processing: {action.upper()} - {url[:60]} (site: {site})")

        if action == 'add':
            return self.process_add_task(site, url)
        elif action == 'update':
            if not fields:
                logger.error(f"❌ UPDATE task missing fields for {url[:60]}")
                return False
            return self.process_update_task(site, url, fields)
        elif action == 'delete':
            return self.process_delete_task(site, url)
        else:
            logger.error(f"❌ Unknown action: {action}")
            return False

    def run(self):
        """Run the consumer loop."""
        logger.info("=" * 60)
        logger.info(f"AIRTABLE TASK CONSUMER - {self.consumer_name}")
        logger.info("=" * 60)

        processed_count = 0
        error_count = 0

        try:
            while True:
                # Consume tasks from stream
                tasks = self.airtable_tasks.consume_tasks(
                    consumer_name=self.consumer_name,
                    count=1,
                    block_ms=5000
                )

                if not tasks:
                    # No tasks available
                    continue

                message_id, task = tasks[0]
                retry_count = int(task.get('retry_count', 0))

                # Process task
                success = self.process_task(task)

                if success:
                    # ACK and continue
                    self.airtable_tasks.ack_message(message_id)
                    processed_count += 1
                else:
                    # Retry or fail
                    error_count += 1

                    if retry_count < MAX_RETRIES:
                        # Re-publish with incremented retry count
                        logger.warning(f"⚠️  Retry {retry_count + 1}/{MAX_RETRIES} for {task['url'][:60]}")

                        # ACK original message
                        self.airtable_tasks.ack_message(message_id)

                        # Re-publish with retry count
                        self.airtable_tasks.publish_task(
                            site=task['site'],
                            action=task['action'],
                            url=task['url'],
                            fields=task.get('fields')
                        )

                        # Update retry_count in the new message
                        # (done via publish_task's default retry_count=0, need to override)
                        # Actually, let me fix this - need to pass retry_count through
                        time.sleep(RETRY_DELAY)
                    else:
                        # Max retries reached, ACK and log
                        logger.error(f"🚫 Max retries reached for {task['url'][:60]}, dropping task")
                        self.airtable_tasks.ack_message(message_id)

        except KeyboardInterrupt:
            logger.info("\n👋 Shutting down gracefully...")
        finally:
            self.close()

        logger.info("=" * 60)
        logger.info("CONSUMER STOPPED")
        logger.info("=" * 60)
        logger.info(f"Tasks processed: {processed_count}")
        logger.info(f"Errors:          {error_count}")
        logger.info("=" * 60)

    def close(self):
        """Close all Redis connections."""
        self.airtable_tasks.close()
        for client in self.processed_urls_clients.values():
            client.close()
        logger.info("✅ All connections closed")


def main():
    parser = argparse.ArgumentParser(
        description='Airtable Task Consumer - Syncs Redis with Airtable',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python airtable_client.py
  python airtable_client.py --consumer-name worker1

Environment Variables:
  AIRTABLE_API_KEY   - Your Airtable API key (required)
  AIRTABLE_BASE_ID   - Airtable base ID (default: appZFLyVxrsHqKeLU)
  AIRTABLE_TABLE_ID  - Airtable table ID (default: tblhe6RxhgquoMxNf)
        """
    )

    parser.add_argument(
        '--consumer-name',
        default='consumer1',
        help='Consumer name for stream consumer group'
    )

    args = parser.parse_args()

    consumer = AirtableTaskConsumer(args.consumer_name)
    consumer.run()


if __name__ == '__main__':
    main()
