#!/usr/bin/env python3
"""
Redis Watcher - Monitor Redis data structures for a site

Usage:
    python redis_watcher.py --site imovelweb
    python redis_watcher.py --site imovelweb --refresh 5  # Auto-refresh every 5 seconds
"""

import argparse
import redis
import json
import time
import os
from datetime import datetime
from typing import Dict, Any, Optional

# Redis connection config
REDIS_HOST = '5.161.248.214'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redispass'

def clear_screen():
    """Clear terminal screen"""
    os.system('clear' if os.name != 'nt' else 'cls')

def format_size(num: int) -> str:
    """Format number with thousands separators"""
    return f"{num:,}"

def truncate_string(s: str, length: int = 100) -> str:
    """Truncate string to max length"""
    if len(s) <= length:
        return s
    return s[:length-3] + "..."

def get_redis_client(db: int = 0) -> redis.Redis:
    """Create Redis client connection"""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=db,
        decode_responses=True
    )

def get_hash_info(client: redis.Redis, key: str) -> Dict[str, Any]:
    """Get info about a Redis Hash"""
    try:
        size = client.hlen(key)

        # Get one example
        example = None
        if size > 0:
            # Get first key
            keys = client.hkeys(key)
            if keys:
                first_key = keys[0]
                value = client.hget(key, first_key)

                # Try to parse as JSON
                try:
                    parsed_value = json.loads(value)
                    example = {
                        'key': first_key,
                        'value': parsed_value
                    }
                except:
                    example = {
                        'key': first_key,
                        'value': value
                    }

        return {
            'exists': True,
            'type': 'Hash',
            'size': size,
            'example': example
        }
    except Exception as e:
        return {
            'exists': False,
            'error': str(e)
        }

def get_stream_info(client: redis.Redis, key: str) -> Dict[str, Any]:
    """Get info about a Redis Stream"""
    try:
        info = client.xinfo_stream(key)

        # Get one example message
        example = None
        if info['length'] > 0:
            messages = client.xrange(key, count=1)
            if messages:
                msg_id, fields = messages[0]
                example = {
                    'message_id': msg_id,
                    'fields': fields
                }

        # Get consumer group info
        groups = []
        try:
            groups_info = client.xinfo_groups(key)
            for group in groups_info:
                groups.append({
                    'name': group['name'],
                    'consumers': group['consumers'],
                    'pending': group['pending']
                })
        except:
            pass

        return {
            'exists': True,
            'type': 'Stream',
            'length': info['length'],
            'groups': groups,
            'example': example
        }
    except redis.exceptions.ResponseError:
        return {
            'exists': False,
            'type': 'Stream'
        }
    except Exception as e:
        return {
            'exists': False,
            'error': str(e)
        }

def print_separator(char='=', length=80):
    """Print separator line"""
    print(char * length)

def print_section_header(title: str):
    """Print section header"""
    print()
    print_separator()
    print(f"  {title}")
    print_separator()

def print_hash_details(name: str, info: Dict[str, Any]):
    """Print details about a Hash"""
    print(f"\n📦 {name}")

    if not info['exists']:
        print("   ❌ Does not exist")
        if 'error' in info:
            print(f"   Error: {info['error']}")
        return

    print(f"   Type: {info['type']}")
    print(f"   Size: {format_size(info['size'])} keys")

    if info['example']:
        print(f"\n   Example Record:")
        print(f"   Key: {truncate_string(info['example']['key'], 70)}")
        print(f"   Value:")

        if isinstance(info['example']['value'], dict):
            for k, v in info['example']['value'].items():
                if isinstance(v, (str, int, float)):
                    print(f"      {k}: {truncate_string(str(v), 60)}")
                else:
                    print(f"      {k}: {type(v).__name__}")
        else:
            print(f"      {truncate_string(str(info['example']['value']), 70)}")

def print_stream_details(name: str, info: Dict[str, Any]):
    """Print details about a Stream"""
    print(f"\n📡 {name}")

    if not info['exists']:
        print("   ❌ Does not exist")
        if 'error' in info:
            print(f"   Error: {info['error']}")
        return

    print(f"   Type: {info['type']}")
    print(f"   Length: {format_size(info['length'])} messages")

    if info.get('groups'):
        print(f"\n   Consumer Groups:")
        for group in info['groups']:
            print(f"      • {group['name']}")
            print(f"        Consumers: {group['consumers']}, Pending: {group['pending']}")

    if info['example']:
        print(f"\n   Example Message:")
        print(f"   ID: {info['example']['message_id']}")
        print(f"   Fields:")
        for k, v in info['example']['fields'].items():
            print(f"      {k}: {truncate_string(str(v), 60)}")

def watch_site(site_name: str, refresh: Optional[int] = None):
    """Watch Redis data structures for a site"""

    # Create clients for both DBs
    client_db0 = get_redis_client(db=0)
    client_db1 = get_redis_client(db=1)

    try:
        while True:
            if refresh:
                clear_screen()

            print_separator('=', 80)
            print(f"  REDIS WATCHER - {site_name.upper()}")
            print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print_separator('=', 80)

            print(f"\n🔗 Connected to: {REDIS_HOST}:{REDIS_PORT}")

            # Scrape Session (Hash - DB 0)
            print_section_header("SCRAPER DATA STRUCTURES (DB 0)")

            scrape_session = get_hash_info(client_db0, f"scrape_session_{site_name}")
            print_hash_details(f"scrape_session_{site_name}", scrape_session)

            # Processed URLs (Hash - DB 0)
            print()
            processed_urls = get_hash_info(client_db0, f"processed_urls_{site_name}")
            print_hash_details(f"processed_urls_{site_name}", processed_urls)

            # URL Stream (Stream - DB 0)
            print()
            url_stream = get_stream_info(client_db0, f"urls_stream_{site_name}")
            print_stream_details(f"urls_stream_{site_name}", url_stream)

            # Failed URLs (Hash - DB 0)
            print()
            failed_urls = get_hash_info(client_db0, f"failed_urls_{site_name}")
            print_hash_details(f"failed_urls_{site_name}", failed_urls)

            # DB Sync (Hash - DB 1)
            print_section_header("DATABASE SYNC (DB 1)")

            db_sync = get_hash_info(client_db1, f"db_sync_{site_name}")
            print_hash_details(f"db_sync_{site_name}", db_sync)

            # Summary
            print_section_header("SUMMARY")

            total_urls = processed_urls.get('size', 0)
            session_urls = scrape_session.get('size', 0)
            stream_length = url_stream.get('length', 0)
            failed_count = failed_urls.get('size', 0)

            print(f"\n   Total Processed:     {format_size(total_urls)}")
            print(f"   Current Session:     {format_size(session_urls)}")
            print(f"   Stream Queue:        {format_size(stream_length)}")
            print(f"   Failed:              {format_size(failed_count)}")

            if total_urls > 0 and session_urls > 0:
                expired = total_urls - session_urls
                print(f"   Potentially Expired: {format_size(expired)}")

            print()
            print_separator('=', 80)

            if not refresh:
                break

            print(f"\n⏱️  Auto-refreshing every {refresh} seconds... (Ctrl+C to stop)")
            time.sleep(refresh)

    except KeyboardInterrupt:
        print("\n\n👋 Stopped watching")
    finally:
        client_db0.close()
        client_db1.close()

def main():
    parser = argparse.ArgumentParser(
        description='Watch Redis data structures for a site',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python redis_watcher.py --site imovelweb
  python redis_watcher.py --site imovelweb --refresh 5
  python redis_watcher.py --site dfimoveis --refresh 10
        """
    )

    parser.add_argument(
        '--site',
        required=True,
        help='Site name (e.g., imovelweb, dfimoveis)'
    )

    parser.add_argument(
        '--refresh',
        type=int,
        help='Auto-refresh interval in seconds'
    )

    args = parser.parse_args()

    watch_site(args.site, args.refresh)

if __name__ == '__main__':
    main()
