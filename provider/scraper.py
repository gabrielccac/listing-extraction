#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Simple Thread-Based Scraper using new Redis-only architecture
"""
import time, logging, signal, sys, threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from scraper_worker import ImovelwebScraper
from redis_client import create_redis_clients  # CHANGED: New import

# Config
SITE_NAME = "imovelweb"
MAX_WORKERS = 3
SUB_REGIONS = ["brasilia-df"]
TRANSACTION_TYPES = ["venda", "aluguel"]
MAX_RETRIES = 3
MAX_NAV_RETRIES = 3  # Retries for navigation failures
PAGES_PER_CHUNK = 150
MAX_PAGES = 600

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(threadName)s] - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
for l in ["seleniumbase", "selenium", "pika"]: logging.getLogger(l).setLevel(logging.WARNING)

# Globals
shutdown_event = threading.Event()
redis_clients = None  # CHANGED: Now multiple Redis clients
stats_lock = threading.Lock()
total_stats = {'pages': 0, 'urls': 0, 'new': 0, 'price_changes': 0, 'duplicates': 0, 'errors': 0}  # CHANGED: Stats names

def signal_handler(sig, frame):
    logger.info("Shutdown requested")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def detect_phase() -> dict:
    """
    Phase 1: Detect total pages for all region+transaction combinations.
    """
    logger.info("="*60)
    logger.info("🔍 PHASE 1: DETECTING TOTAL PAGES")
    logger.info("="*60)

    detection_results = {}
    total_combinations = len(SUB_REGIONS) * len(TRANSACTION_TYPES)
    current = 0

    for transaction_type in TRANSACTION_TYPES:
        for sub_region in SUB_REGIONS:
            current += 1
            task_key = f"{sub_region}_{transaction_type}"

            logger.info(f"[{current}/{total_combinations}] Detecting {task_key}...")

            scraper = None
            try:
                # CHANGED: Create scraper with Redis clients
                scraper = ImovelwebScraper(redis_clients)
                scraper.init_browser(headless=False)

                # Navigate to page 1
                task = {'sub_region': sub_region, 'transaction_type': transaction_type}
                url = scraper.get_page_url(task, 1)

                if not scraper.navigate(url):
                    logger.error(f"✗ {task_key}: Failed to navigate")
                    detection_results[task_key] = 0
                    continue

                # Detect total pages
                total_pages = scraper.detect_total_pages()

                if total_pages == 0:
                    logger.warning(f"✗ {task_key}: No pages detected")
                else:
                    logger.info(f"✓ {task_key}: {total_pages} pages")

                detection_results[task_key] = total_pages

            except Exception as e:
                logger.error(f"✗ {task_key}: Error - {str(e)[:100]}")
                detection_results[task_key] = 0

            finally:
                if scraper:
                    try:
                        scraper.close_browser()
                    except:
                        pass

            time.sleep(1)

    # Summary
    total_pages = sum(detection_results.values())
    successful = sum(1 for pages in detection_results.values() if pages > 0)

    logger.info("="*60)
    logger.info(f"✓ Detection complete: {successful}/{total_combinations} successful")
    logger.info(f"✓ Total pages to scrape: {total_pages:,}")
    logger.info("="*60)

    return detection_results

def generate_chunks(detection_results: dict) -> list:
    """
    Phase 2: Generate scraping chunks from detection results.
    (UNCHANGED - same logic)
    """
    logger.info("="*60)
    logger.info("📋 PHASE 2: GENERATING CHUNKS")
    logger.info("="*60)

    chunks = []

    for task_key, total_pages in detection_results.items():
        if total_pages == 0:
            logger.info(f"⊘ Skipping {task_key} - 0 pages detected")
            continue

        parts = task_key.rsplit('_', 1)
        if len(parts) != 2:
            logger.warning(f"✗ Invalid task_key format: {task_key}")
            continue

        sub_region, transaction_type = parts
        effective_pages = min(total_pages, MAX_PAGES)
        num_chunks = (effective_pages + PAGES_PER_CHUNK - 1) // PAGES_PER_CHUNK

        for i in range(num_chunks):
            start_page = i * PAGES_PER_CHUNK + 1
            end_page = min((i + 1) * PAGES_PER_CHUNK, effective_pages)

            chunk = {
                'sub_region': sub_region,
                'transaction_type': transaction_type,
                'start_page': start_page,
                'end_page': end_page,
                'total_pages': effective_pages,
                'task_key': f"{task_key}_{start_page}-{end_page}"
            }

            chunks.append(chunk)

            if num_chunks > 1:
                logger.info(f"✓ {task_key}: Chunk {i+1}/{num_chunks} (pages {start_page}-{end_page})")
            else:
                logger.info(f"✓ {task_key}: Single chunk (pages {start_page}-{end_page})")

    logger.info("="*60)
    logger.info(f"✓ Generated {len(chunks)} chunks from {len([p for p in detection_results.values() if p > 0])} tasks")
    logger.info("="*60)

    return chunks

def click_next_with_verification(scraper, expected_page_num: int, max_retries: int = 3) -> bool:
    """
    Click next page button and verify we landed on the expected page.
    (UNCHANGED - same logic)
    """
    for attempt in range(max_retries):
        if shutdown_event.is_set():
            logger.debug("Shutdown requested, aborting click navigation")
            return False

        try:
            current_url = scraper.sb.get_current_url()

            if not scraper.click_next_page():
                logger.debug(f"Next button not clickable (attempt {attempt+1}/{max_retries})")
                continue

            url_changed = False
            start_wait = time.time()

            while time.time() - start_wait < 8:
                if shutdown_event.is_set():
                    return False

                new_url = scraper.sb.get_current_url()
                if new_url != current_url:
                    url_changed = True
                    break

                time.sleep(0.3)

            if not url_changed:
                logger.debug(f"URL didn't change after click (attempt {attempt+1}/{max_retries})")
                continue

            new_url = scraper.sb.get_current_url()

            if f"pagina-{expected_page_num}" in new_url:
                logger.debug(f"✓ Clicked to page {expected_page_num}")
                return True
            else:
                logger.debug(f"URL doesn't contain 'pagina-{expected_page_num}': {new_url}")
                if expected_page_num == 1 and '-pagina-' not in new_url:
                    logger.debug(f"✓ On page 1 (no pagina suffix)")
                    return True

        except Exception as e:
            logger.debug(f"Click error (attempt {attempt+1}/{max_retries}): {str(e)[:100]}")

        if attempt < max_retries - 1:
            time.sleep(1)

    return False

def scrape_task(task):
    """
    Scrape a chunk of pages for a region+transaction.
    
    CHANGED: Uses new Redis clients and store_urls_batch signature
    """
    key = task['task_key']
    start_page = task['start_page']
    end_page = task['end_page']
    total_pages = task['total_pages']
    scraper = None

    try:
        # CHANGED: Create scraper with Redis clients
        scraper = ImovelwebScraper(redis_clients)
        scraper.init_browser(headless=False)

        logger.info(f"[{key}] Starting chunk: pages {start_page}-{end_page} of {total_pages}")

        # CHANGED: No session_hash needed anymore
        metadata = {'sub_region': task['sub_region'], 'transaction_type': task['transaction_type']}

        # Navigate to first page of chunk with retries
        first_page_url = scraper.get_page_url(task, start_page)
        nav_success = False
        for attempt in range(MAX_NAV_RETRIES):
            if scraper.navigate(first_page_url):
                nav_success = True
                break

            logger.warning(f"[{key}] Navigation attempt {attempt+1}/{MAX_NAV_RETRIES} failed for start page {start_page}")

            if attempt < MAX_NAV_RETRIES - 1:
                logger.info(f"[{key}] Restarting browser and retrying...")
                scraper.restart_browser()

        if not nav_success:
            logger.error(f"[{key}] Failed to navigate to start page {start_page} after {MAX_NAV_RETRIES} attempts, skipping chunk")
            return

        # Loop through all pages in chunk
        for page in range(start_page, end_page + 1):
            if shutdown_event.is_set():
                logger.info(f"[{key}] Shutdown requested at page {page}")
                break

            # Extract URLs from current page with retry logic
            urls = []
            for attempt in range(MAX_RETRIES):
                try:
                    urls = scraper.extract_page_data()
                    if urls:
                        break
                except Exception as e:
                    logger.error(f"[{key}] Page {page} extraction error (attempt {attempt+1}): {e}")

                if attempt < MAX_RETRIES - 1:
                    logger.debug(f"[{key}] Restarting browser and retrying page {page}")
                    scraper.restart_browser()
                    scraper.navigate(scraper.get_page_url(task, page))

            # CHANGED: Store URLs with new method signature
            if urls:
                stats = scraper.store_urls_batch(urls, metadata)  # CHANGED: No session_hash
                with stats_lock:
                    total_stats['urls'] += len(urls)
                    total_stats['new'] += stats['new']
                    total_stats['price_changes'] += stats['price_changes']
                    total_stats['duplicates'] += stats['duplicates']  # CHANGED: was 'dupes'
                    total_stats['pages'] += 1
                
                # CHANGED: Updated log message
                logger.info(f"[{key}] Page {page}/{total_pages}: {len(urls)} URLs | "
                           f"🆕{stats['new']} 💰{stats['price_changes']} 🔄{stats['duplicates']}")
            else:
                with stats_lock:
                    total_stats['errors'] += 1
                logger.warning(f"[{key}] Page {page}/{total_pages}: No URLs extracted after {MAX_RETRIES} attempts")

            # Navigate to next page (if not last page in chunk)
            if page < end_page:
                expected_next_page = page + 1

                if click_next_with_verification(scraper, expected_next_page):
                    logger.debug(f"[{key}] ✓ Clicked to page {expected_next_page}")
                else:
                    logger.warning(f"[{key}] Click failed, using direct URL for page {expected_next_page}")
                    next_page_url = scraper.get_page_url(task, expected_next_page)

                    # Try navigation with retries
                    nav_success = False
                    for attempt in range(MAX_NAV_RETRIES):
                        if scraper.navigate(next_page_url):
                            nav_success = True
                            break

                        logger.warning(f"[{key}] Navigation attempt {attempt+1}/{MAX_NAV_RETRIES} failed for page {expected_next_page}")

                        if attempt < MAX_NAV_RETRIES - 1:
                            logger.info(f"[{key}] Restarting browser and retrying...")
                            scraper.restart_browser()

                    if not nav_success:
                        logger.error(f"[{key}] Failed to navigate to page {expected_next_page} after {MAX_NAV_RETRIES} attempts, stopping chunk")
                        break

                time.sleep(1)
        
        logger.info(f"[{key}] Complete")
        
    except Exception as e:
        logger.error(f"[{key}] Failed: {e}")
    finally:
        if scraper:
            try:
                scraper.close_browser()
            except:
                pass

def main():
    global redis_clients  # CHANGED: Now multiple Redis clients

    print(f"\n{'='*60}\n {SITE_NAME.upper()} SCRAPER - REDIS-ONLY ARCHITECTURE\n{'='*60}")  # CHANGED: Title
    print(f" Workers: {MAX_WORKERS} | Regions: {len(SUB_REGIONS)} | Transactions: {len(TRANSACTION_TYPES)}")
    print(f" Chunk Size: {PAGES_PER_CHUNK} pages | Max Pages: {MAX_PAGES}")
    print(f"{'='*60}\n")

    start_time = time.time()

    # ========================================================================
    # SETUP: Connect to Redis - COMPLETELY CHANGED
    # ========================================================================

    try:
        # CHANGED: Create all Redis clients
        redis_clients = create_redis_clients(
            site_name=SITE_NAME,
            host="5.161.248.214",  # Your Redis host
            port=6379,
            password="redispass"   # Your Redis password
        )
        
        # Clear previous scrape session at start
        redis_clients['scrape_session'].clear_session()
        logger.info("✅ Redis clients connected and session cleared")
        
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        sys.exit(1)

    # ========================================================================
    # PHASE 1: Detect Total Pages
    # ========================================================================

    detection_results = detect_phase()

    valid_tasks = {k: v for k, v in detection_results.items() if v > 0}
    if not valid_tasks:
        logger.error("✗ No valid tasks detected! Exiting.")
        sys.exit(1)

    # ========================================================================
    # PHASE 2: Generate Chunks
    # ========================================================================

    chunks = generate_chunks(detection_results)

    if not chunks:
        logger.error("✗ No chunks generated! Exiting.")
        sys.exit(1)

    # ========================================================================
    # PHASE 3: Execute Chunks in Parallel
    # ========================================================================

    logger.info("="*60)
    logger.info(f"🚀 PHASE 3: EXECUTING {len(chunks)} CHUNKS")
    logger.info("="*60)

    completed = 0
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(scrape_task, chunk): chunk for chunk in chunks}
            
            while futures:
                if shutdown_event.is_set():
                    for f in futures:
                        f.cancel()
                    break
                
                done, not_done = wait(futures, timeout=1.0, return_when=FIRST_COMPLETED)
                
                for f in done:
                    try:
                        f.result()
                        completed += 1
                        logger.info(f"Progress: {completed}/{len(chunks)} chunks complete")
                    except Exception as e:
                        logger.error(f"Chunk error: {e}")
                
                futures = {f: futures[f] for f in not_done}
    
    except KeyboardInterrupt:
        shutdown_event.set()
    
    # CHANGED: No bulk publishing needed - URLs published in real-time to Redis Stream
    if not shutdown_event.is_set():
        # Just log stream statistics
        pending_count = redis_clients['url_stream'].get_pending_count()
        processed_count = len(redis_clients['processed_urls'].get_all_urls())
        logger.info(f"📊 Stream stats: {pending_count} pending URLs, {processed_count} total processed")
    
    # Cleanup
    try:
        # Close all Redis connections
        for name, client in redis_clients.items():
            client.close()
        logger.info("✅ All Redis connections closed")
    except:
        pass
    
    elapsed = time.time() - start_time

    print(f"\n{'='*60}\n SCRAPING COMPLETE\n{'='*60}")
    print(f" Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f" Chunks: {completed}/{len(chunks)} completed")
    print(f" Pages: {total_stats['pages']:,} | URLs: {total_stats['urls']:,}")
    print(f" New: {total_stats['new']:,} | Price Changes: {total_stats['price_changes']:,}")
    print(f" Duplicates: {total_stats['duplicates']:,} | Errors: {total_stats['errors']}")  # CHANGED: Stat name
    print(f"{'='*60}\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        sys.exit(1)