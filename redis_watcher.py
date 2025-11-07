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
        return {
            'exists': True,
            'type': 'Hash',
            'size': size
        }
    except Exception as e:
        return {
            'exists': False,
            'error': str(e),
            'type': 'Hash',
            'size': 0
        }

def get_stream_info(client: redis.Redis, key: str) -> Dict[str, Any]:
    """Get info about a Redis Stream"""
    try:
        info = client.xinfo_stream(key)
        
        # Get consumer groups info
        groups_count = 0
        pending_total = 0
        try:
            groups_info = client.xinfo_groups(key)
            groups_count = len(groups_info)
            for group in groups_info:
                pending_total += group['pending']
        except:
            pass

        return {
            'exists': True,
            'type': 'Stream',
            'size': info['length'],
            'groups': groups_count,
            'pending': pending_total
        }
    except redis.exceptions.ResponseError:
        return {
            'exists': False,
            'type': 'Stream',
            'size': 0,
            'groups': 0,
            'pending': 0
        }
    except Exception as e:
        return {
            'exists': False,
            'error': str(e),
            'type': 'Stream',
            'size': 0,
            'groups': 0,
            'pending': 0
        }

def print_table_header():
    """Print compressed table header"""
    print(f"{'DATA STRUCTURE':<30} | {'TYPE':<10} | {'SIZE':>10} | {'GROUPS':>8} | {'PENDING':>8}")
    print('-' * 80)

def print_table_row(name: str, info: Dict[str, Any]):
    """Print a compressed table row"""
    status = "✅" if info['exists'] else "❌"
    name_display = f"{status} {name}"
    
    if not info['exists']:
        print(f"{name_display:<30} | {info.get('type', 'Unknown'):<10} | {'N/A':>10} | {'N/A':>8} | {'N/A':>8}")
        return
    
    size_str = format_size(info.get('size', 0))
    groups_str = str(info.get('groups', 0)) if info.get('groups', 0) > 0 else "-"
    pending_str = format_size(info.get('pending', 0)) if info.get('pending', 0) > 0 else "-"
    
    print(f"{name_display:<30} | {info.get('type', 'Unknown'):<10} | {size_str:>10} | {groups_str:>8} | {pending_str:>8}")

def watch_site(site_name: str, refresh: Optional[int] = None):
    """Watch Redis data structures for a site"""

    # Create clients for both DBs
    client_db0 = get_redis_client(db=0)
    client_db1 = get_redis_client(db=1)

    try:
        while True:
            if refresh:
                clear_screen()

            # Header
            print('=' * 80)
            print(f"REDIS WATCHER - {site_name.upper()}")
            print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            if refresh:
                print(f"Refresh: {refresh}s")
            print('=' * 80)

            # Scraper Data Structures (DB 0)
            print(f"\n📊 SCRAPER DATA STRUCTURES (DB 0)")
            print_table_header()
            
            scrape_session = get_hash_info(client_db0, f"scrape_session_{site_name}")
            print_table_row(f"scrape_session_{site_name}", scrape_session)
            
            processed_urls = get_hash_info(client_db0, f"processed_urls_{site_name}")
            print_table_row(f"processed_urls_{site_name}", processed_urls)
            
            url_stream = get_stream_info(client_db0, f"urls_stream_{site_name}")
            print_table_row(f"urls_stream_{site_name}", url_stream)
            
            failed_urls = get_hash_info(client_db0, f"failed_urls_{site_name}")
            print_table_row(f"failed_urls_{site_name}", failed_urls)

            # Database Sync (DB 1)
            print(f"\n🔄 DATABASE SYNC (DB 1)")
            print_table_header()
            
            db_sync = get_hash_info(client_db1, f"db_sync_{site_name}")
            print_table_row(f"db_sync_{site_name}", db_sync)

            # Quick Stats
            print(f"\n📈 QUICK STATS")
            print('-' * 40)
            total_processed = processed_urls.get('size', 0)
            current_session = scrape_session.get('size', 0)
            stream_queue = url_stream.get('size', 0)
            failed_count = failed_urls.get('size', 0)
            
            print(f"Processed URLs:  {format_size(total_processed)}")
            print(f"Active Session:  {format_size(current_session)}")
            print(f"Stream Queue:    {format_size(stream_queue)}")
            print(f"Failed URLs:     {format_size(failed_count)}")
            
            if total_processed > 0 and current_session > 0:
                expired = total_processed - current_session
                print(f"Expired URLs:    {format_size(expired)}")

            print('=' * 80)

            if not refresh:
                break

            print(f"\n⏱️  Refreshing in {refresh} seconds... (Ctrl+C to stop)")
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