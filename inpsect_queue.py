#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Queue URL Inspector

Quick script to peek at URLs in the RabbitMQ queue without consuming them.
"""
import pika

# Configuration
RABBITMQ_HOST = '5.161.248.214'
RABBITMQ_USER = 'admin'
RABBITMQ_PASSWORD = 'rabbitmqpass'
SITE_NAME = 'imovelweb'
QUEUE_NAME = f'scraped_urls_{SITE_NAME}'

# How many URLs to inspect
NUM_URLS = 20

print(f"\n{'='*80}")
print(f" Inspecting URLs from queue: {QUEUE_NAME}")
print(f"{'='*80}\n")

try:
    # Connect to RabbitMQ
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    )
    channel = connection.channel()
    
    # Get queue size
    queue_info = channel.queue_declare(queue=QUEUE_NAME, durable=True, passive=True)
    total_in_queue = queue_info.method.message_count
    print(f"📊 Total URLs in queue: {total_in_queue:,}\n")
    
    if total_in_queue == 0:
        print("❌ Queue is empty!")
        connection.close()
        exit(0)
    
    # Fetch URLs
    urls_to_inspect = min(NUM_URLS, total_in_queue)
    print(f"🔍 Inspecting first {urls_to_inspect} URLs:\n")
    print(f"{'#':<4} {'URL':<76}")
    print("-" * 80)
    
    inspected_urls = []
    
    for i in range(urls_to_inspect):
        method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=False)
        
        if method_frame:
            url = body.decode('utf-8')
            inspected_urls.append((method_frame.delivery_tag, url))
            
            # Check for issues
            issues = []
            if '//' in url and url.count('//') > 1:  # More than just https://
                issues.append('⚠️  DOUBLE SLASH')
            if not url.startswith('http'):
                issues.append('❌ NO PROTOCOL')
            if ' ' in url:
                issues.append('❌ SPACE IN URL')
            
            issue_str = ' '.join(issues) if issues else '✅'
            
            # Truncate URL for display
            display_url = url if len(url) <= 70 else url[:67] + '...'
            
            print(f"{i+1:<4} {display_url:<76} {issue_str}")
    
    print("-" * 80)
    
    # Requeue all URLs (so they're not consumed)
    print(f"\n🔄 Requeuing {len(inspected_urls)} URLs back to queue...")
    for delivery_tag, url in inspected_urls:
        channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
    
    print("✅ All URLs requeued successfully!\n")
    
    # Summary
    print(f"{'='*80}")
    print(" SUMMARY")
    print(f"{'='*80}")
    
    double_slash_count = sum(1 for _, url in inspected_urls if url.count('//') > 1)
    no_protocol_count = sum(1 for _, url in inspected_urls if not url.startswith('http'))
    space_count = sum(1 for _, url in inspected_urls if ' ' in url)
    
    print(f"  Total inspected:   {len(inspected_urls)}")
    print(f"  Double slashes:    {double_slash_count}")
    print(f"  No protocol:       {no_protocol_count}")
    print(f"  Spaces in URL:     {space_count}")
    print(f"  Clean URLs:        {len(inspected_urls) - double_slash_count - no_protocol_count - space_count}")
    print(f"{'='*80}\n")
    
    # Show some examples if issues found
    if double_slash_count > 0:
        print("📋 Example URLs with double slashes:")
        for _, url in inspected_urls[:3]:
            if url.count('//') > 1:
                print(f"   {url}")
        print()
    
    connection.close()

except Exception as e:
    print(f"\n❌ Error: {e}\n")
    exit(1)