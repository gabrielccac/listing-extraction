#!/usr/bin/env python3
import argparse
import redis

REDIS_HOST = '5.161.248.214'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redispass'

def clear_redis_data(site: str):
    # Clear DB 0 (main data)
    client_db0 = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, 
        password=REDIS_PASSWORD, db=0, decode_responses=True
    )
    
    # Clear DB 1 (sync data)
    client_db1 = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, 
        password=REDIS_PASSWORD, db=1, decode_responses=True
    )
    
    keys_db0 = [
        f"scrape_session_{site}",
        f"processed_urls_{site}",
        f"urls_stream_{site}",
        f"failed_urls_{site}"
    ]
    
    keys_db1 = [
        f"db_sync_{site}"
    ]
    
    print("Clearing DB 0:")
    for key in keys_db0:
        deleted = client_db0.delete(key)
        print(f"{'✅' if deleted else '❌'} {key}")
    
    print("\nClearing DB 1:")
    for key in keys_db1:
        deleted = client_db1.delete(key)
        print(f"{'✅' if deleted else '❌'} {key}")
    
    client_db0.close()
    client_db1.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', required=True)
    args = parser.parse_args()
    
    clear_redis_data(args.site)