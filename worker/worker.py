import time
import logging
import threading
import os
import sys
import signal
import csv
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

# Import from our modules
from queue_client import create_queue_manager, create_redis_deduplicator
from parser import parse_property_data

# Import pika and redis for compatibility with existing code
import pika

# Site-specific configuration
SITE_NAME = 'imovelweb'
REDIS_PROCESSED_SET = f'processed_urls_{SITE_NAME}'

# --- Configuration ---

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(threadName)s] - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
for l in ["seleniumbase", "selenium", "pika"]: logging.getLogger(l).setLevel(logging.WARNING)

# Configuration
MAX_RETRIES = 3
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '3'))

# CSV Output file - unique per consumer instance
CONSUMER_ID = os.getpid()
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '.')
CSV_OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'imovelweb_listings_{CONSUMER_ID}.csv')
FAILED_DIR = "failed_urls"

# Ensure directories exist
os.makedirs(FAILED_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Thread-safe storage & stats
results_lock = threading.Lock()
mq_lock = threading.Lock()
total_stats = {
    'success': 0,
    'errors': 0,
    'captchas_solved': 0,
    'browser_restarts': 0,
    'offline': 0,
    'rows_written': 0,
}

# NEW: Track all browser instances for force-close on shutdown
active_browsers = []
browsers_lock = threading.Lock()

# Global flag for graceful shutdown
shutdown_event = threading.Event()
force_shutdown = False  # NEW: Track force shutdown (second Ctrl+C)

# NEW: Function to force-close all browsers
def close_all_browsers():
    """Force-close all active browsers (called on shutdown)."""
    with browsers_lock:
        if not active_browsers:
            return
        
        logger.info(f"[SHUTDOWN] Force-closing {len(active_browsers)} browser(s)...")
        
        for browser in active_browsers:
            try:
                browser.driver.stop()
            except Exception as e:
                logger.debug(f"Error force-closing browser: {e}")
        
        active_browsers.clear()
        logger.info("[SHUTDOWN] All browsers closed")

# Signal handler for Ctrl+C
def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global force_shutdown
    
    if shutdown_event.is_set():
        # Second Ctrl+C - force shutdown
        logger.warning("\n[FORCE SHUTDOWN] Second Ctrl+C detected!")
        logger.warning("[FORCE SHUTDOWN] Closing all browsers and exiting immediately...")
        force_shutdown = True
        close_all_browsers()
        sys.exit(1)
    
    # First Ctrl+C - graceful shutdown
    shutdown_event.set()
    logger.info("\n[SHUTDOWN] Ctrl+C detected! Initiating graceful shutdown...")
    logger.info("[SHUTDOWN] Workers will finish current URL and exit")
    logger.info("[SHUTDOWN] Press Ctrl+C again to force quit immediately")
    
    # Close browsers after a timeout if workers don't finish
    def delayed_browser_close():
        time.sleep(10)  # Wait 10 seconds for graceful shutdown
        if not force_shutdown:
            logger.warning("[SHUTDOWN] Timeout reached - force-closing browsers")
            close_all_browsers()
    
    threading.Thread(target=delayed_browser_close, daemon=True).start()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- CSV Output ---
def save_to_csv(property_data):
    """Save property data to CSV file."""
    file_exists = os.path.isfile(CSV_OUTPUT_FILE)

    fieldnames = [
        "url_imovel", "_source", "short_id", "tipo_imovel", "enquadramento", "purpose_type",
        "stage_description", "titulo", "descricao", "endereco", "bairro", "cidade",
        "anunciante", "anunciante_id", "anunciante_creci", "responsavel", "tipo_responsavel",
        "contato_responsavel", "contatos_adicionais", "picture_link", "carousel_img_links",
        "metragem", "quartos", "suites", "banheiro", "vagas", "valor", "condominio", "iptu",
        "preco_m2", "aceita_permuta", "aceita_fgts", "aceita_financiamento", "mobiliado",
        "piso", "posicao_solar", "andar", "elevador", "tipo_cozinha", "vazado", "dce",
        "reformado", "vista_livre", "varanda", "gas_encanado", "reforma_hidraulica",
        "reforma_eletrica", "fachada_reformada", "lazer_completo", "lazer_parcial",
        "sala_ginastica", "salao_de_festas", "salao_de_jogos", "sauna", "piscina",
        "piscina_aquecida", "quadra_esportiva", "churrasqueira", "cinema", "lavanderia",
        "playground", "pista_skate", "features", "dt_atualizacao", "anuncio_id"
    ]

    try:
        with results_lock:
            with open(CSV_OUTPUT_FILE, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if not file_exists:
                    writer.writeheader()

                row = {field: property_data.get(field, "") for field in fieldnames}

                # Convert lists to strings for CSV
                if row.get("contatos_adicionais") and isinstance(row["contatos_adicionais"], list):
                    row["contatos_adicionais"] = ", ".join(row["contatos_adicionais"])
                
                if row.get("carousel_img_links") and isinstance(row["carousel_img_links"], list):
                    row["carousel_img_links"] = ", ".join(row["carousel_img_links"])
                
                if row.get("features") and isinstance(row["features"], list):
                    row["features"] = ", ".join(row["features"])

                writer.writerow(row)

        total_stats['rows_written'] += 1
        return True
        
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        return False

# --- Captcha Handling Functions ---
def is_captcha_page(sb):
    """Check if current page is a captcha page"""
    try:
        title = sb.get_title()
        if "Um momento" in title or "Um momento..." in title:
            return True
        page_source = sb.get_page_source()
        captcha_indicators = [
            "cf-challenge",
            "Verificando se você é humano",
            "Confirme que você é humano"
        ]
        for indicator in captcha_indicators:
            if indicator in page_source:
                return True
    except Exception as e:
        logger.debug(f"Error checking for captcha: {e}")
    return False

def handle_captcha(self, max_wait: int = 30) -> bool:
    """
    Wait for SeleniumBase UC mode to solve captcha.
    
    Args:
        max_wait: Maximum time to wait (seconds)
        
    Returns:
        True if captcha cleared
    """
    logger.warning("⚠️  Captcha detected! Waiting for UC mode to handle it...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait:
        # Check if title changed (captcha cleared)
        try:
            title = self.sb.get_title()
            if "Um momento" not in title:
                logger.info("✅ Captcha cleared successfully")
                return True
        except:
            pass
        
        time.sleep(1)
    
    logger.error("🚫 Captcha not cleared within timeout")
    return False

def is_listing_active(soup, url):
    """Check if the listing is active by confirming the main container exists."""
    try:
        active_container = soup.find('main', class_='bg-ficha')
        if active_container:
            return True
        else:
            logger.warning(f"No active listing container found: {url}")
            return False
    except Exception as e:
        logger.error(f"Error checking listing status for {url}: {e}")
        return False

# --- Worker Function ---
def persistent_worker(worker_id, url_retry_counts, mq_channel, mq_lock, redis_client, queue_name):
    """
    Worker that maintains ONE browser session.
    Pulls URLs directly from RabbitMQ and processes them.
    """
    thread_name = f"Worker-{worker_id}"
    logger.info(f"[{thread_name}] 🚀 Starting persistent worker")

    sb = None
    urls_processed = 0

    try:
        sb = sb_cdp.Chrome(uc=True, uc_cdp_events=True, locale="pt-br")
        
        # NEW: Register browser for tracking
        with browsers_lock:
            active_browsers.append(sb)
        
        logger.info(f"[{thread_name}] ✅ Browser initialized")

        while not shutdown_event.is_set():
            url = None
            retry_count = 0
            delivery_tag = None

            # Get next URL directly from RabbitMQ using the lock
            try:
                with mq_lock:
                    # Check shutdown before attempting RMQ operation
                    if shutdown_event.is_set():
                        logger.info(f"[{thread_name}] 🛑 Shutdown detected, exiting")
                        break
                    
                    method_frame, header_frame, body = mq_channel.basic_get(
                        queue=queue_name, auto_ack=False
                    )
                
                if method_frame:
                    url = body.decode('utf-8')
                    delivery_tag = method_frame.delivery_tag
                else:
                    logger.info(f"[{thread_name}] 📭 Fila vazia. Encerrando worker.")
                    break
            
            except Exception as e:
                if shutdown_event.is_set():
                    logger.debug(f"[{thread_name}] Expected error during shutdown: {e}")
                    break
                logger.error(f"[{thread_name}] ❌ Error fetching from RabbitMQ: {e}")
                time.sleep(5)
                continue

            # Get retry count
            with results_lock:
                retry_count = url_retry_counts.get(url, 0)

            # Check shutdown before processing
            if shutdown_event.is_set():
                logger.info(f"[{thread_name}] 🛑 Shutdown → requeuing {url[:80]}")
                with mq_lock:
                    try:
                        mq_channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                    except:
                        pass
                break

            # Process this URL
            logger.info(f"[{thread_name}] Processing: {url} (Retry {retry_count})")
            url_start_time = time.time()

            try:
                sb.open(url)

                # Check shutdown after navigation
                if shutdown_event.is_set():
                    logger.info(f"[{thread_name}] 🛑 Shutdown during navigation")
                    with mq_lock:
                        try:
                            mq_channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                        except:
                            pass
                    break

                if is_captcha_page(sb):
                    if not handle_captcha(sb):
                        logger.error(f"[{thread_name}] Failed to solve captcha for {url}")
                        handle_failed_url(url, "Captcha resolution failed",
                                          url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name)

                        # Restart browser after captcha failure
                        try:
                            sb.driver.stop()
                            with browsers_lock:
                                if sb in active_browsers:
                                    active_browsers.remove(sb)
                        except:
                            pass

                        try:
                            logger.info(f"[{thread_name}] 🔄 Restarting browser after captcha failure...")
                            sb = sb_cdp.Chrome(uc=True, uc_cdp_events=True, locale="pt-br")
                            with browsers_lock:
                                active_browsers.append(sb)
                            with results_lock:
                                total_stats['browser_restarts'] += 1
                            logger.info(f"[{thread_name}] ✅ Browser restarted successfully")
                        except Exception as restart_error:
                            logger.error(f"[{thread_name}] 🚨 Failed to restart browser: {restart_error}")
                            break

                        continue

                html = sb.get_page_source()
                soup = BeautifulSoup(html, 'lxml')

                if not is_listing_active(soup, url):
                    logger.warning(f"[{thread_name}] 🛑 Listing is OFFLINE, skipping: {url}")
                    with mq_lock:
                        if not shutdown_event.is_set():
                            mq_channel.basic_ack(delivery_tag=delivery_tag)
                    redis_client.srem(REDIS_PROCESSED_SET, url)
                    logger.debug(f"[{thread_name}] Removed offline URL from Redis: {url}")
                    with results_lock:
                        total_stats['offline'] += 1
                    continue

                property_data = parse_property_data(soup, url, html)

                if not property_data or not property_data.get("titulo"):
                    logger.warning(f"[{thread_name}] No valid data found for {url}")
                    sb.save_screenshot(os.path.join(FAILED_DIR, f"no_data_{time.time_ns()}.png"))
                    handle_failed_url(url, "No valid data extracted",
                                      url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name)
                    continue

                if not save_to_csv(property_data):
                    logger.error(f"[{thread_name}] Failed to save to CSV for {url}")
                    handle_failed_url(url, "CSV save failed",
                                      url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name)
                    continue

                # Success!
                url_elapsed = time.time() - url_start_time
                logger.info(f"[{thread_name}] ✅ Success for {url} in {url_elapsed:.2f}s")
                
                with results_lock:
                    total_stats['success'] += 1
                
                if delivery_tag:
                    with mq_lock:
                        if not shutdown_event.is_set():
                            mq_channel.basic_ack(delivery_tag=delivery_tag)
                redis_client.sadd(REDIS_PROCESSED_SET, url)
                
                urls_processed += 1

            except Exception as e:
                logger.error(f"[{thread_name}] ❌ Error processing {url}: {str(e)[:200]}")

                try:
                    sb.driver.stop()
                    with browsers_lock:
                        if sb in active_browsers:
                            active_browsers.remove(sb)
                except:
                    pass

                try:
                    logger.info(f"[{thread_name}] 🔄 Restarting browser...")
                    sb = sb_cdp.Chrome(uc=True, uc_cdp_events=True, locale="pt-br")
                    with browsers_lock:
                        active_browsers.append(sb)
                    with results_lock:
                        total_stats['browser_restarts'] += 1
                    logger.info(f"[{thread_name}] ✅ Browser restarted successfully")
                except Exception as restart_error:
                    logger.error(f"[{thread_name}] 🚨 Failed to restart browser: {restart_error}")
                    if url and delivery_tag:
                        handle_failed_url(url, str(e), url_retry_counts,
                                          mq_channel, mq_lock, delivery_tag, queue_name)
                    break 

                handle_failed_url(url, str(e), url_retry_counts,
                                  mq_channel, mq_lock, delivery_tag, queue_name)

    finally:
        # Clean up browser
        if sb:
            try:
                logger.info(f"[{thread_name}] 🧹 Closing browser (processed {urls_processed} URLs)")
                sb.driver.stop()
                
                # Remove from tracking
                with browsers_lock:
                    if sb in active_browsers:
                        active_browsers.remove(sb)
                        
            except Exception as e:
                logger.debug(f"[{thread_name}] Error closing browser: {e}")
        
        logger.info(f"[{thread_name}] 👋 Worker exiting")

def handle_failed_url(url, error_msg, url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name):
    """Handle a failed URL with retry logic"""
    if shutdown_event.is_set():
        return  # Don't retry during shutdown
    
    with results_lock:
        total_stats['errors'] += 1
        retry_count = url_retry_counts.get(url, 0)

        if retry_count < MAX_RETRIES:
            url_retry_counts[url] = retry_count + 1

            with mq_lock:
                if shutdown_event.is_set():
                    return
                
                try:
                    if delivery_tag:
                        mq_channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                    
                    mq_channel.basic_publish(
                        exchange='',
                        routing_key=queue_name,
                        body=url.encode('utf-8'),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    logger.info(f"🔄 Re-queued {url} to RabbitMQ (Retry {retry_count + 1}/{MAX_RETRIES})")
                except Exception as e:
                    logger.debug(f"Error requeuing: {e}")
        else:
            logger.error(f"🚫 Max retries reached for {url}, discarding.")
            if delivery_tag:
                with mq_lock:
                    try:
                        if not shutdown_event.is_set():
                            mq_channel.basic_ack(delivery_tag=delivery_tag)
                    except:
                        pass

# --- Main Thread ---

if __name__ == "__main__":
    start_time = time.time()

    logger.info("Connecting to RabbitMQ and Redis...")

    try:
        queue_mgr = create_queue_manager(SITE_NAME)
        queue_mgr.connect()
        mq_connection = queue_mgr.connection
        mq_channel = queue_mgr.channel
        logger.info("✅ Connected to RabbitMQ")
    except Exception as e:
        logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
        logger.error("Cannot proceed without RabbitMQ. Exiting.")
        exit(1)

    try:
        redis_dedup = create_redis_deduplicator(SITE_NAME)
        redis_client = redis_dedup.client
        logger.info("✅ Connected to Redis")
        logger.info(f"   Processed URLs in Redis: {redis_dedup.get_total_count()}")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")
        logger.error("Cannot proceed without Redis. Exiting.")
        mq_connection.close()
        exit(1)

    url_retry_counts = {}
    queue_name = queue_mgr.queue_name

    logger.info("=" * 60)
    logger.info(f"Starting ImovelWeb Persistent Session Processor")
    logger.info(f"Consumer ID: {CONSUMER_ID} | Max Workers: {MAX_WORKERS}")
    logger.info(f"Queue: {queue_name}")
    logger.info(f"Output file: {CSV_OUTPUT_FILE}")
    logger.info("=" * 60)
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for worker_id in range(MAX_WORKERS):
                future = executor.submit(
                    persistent_worker,
                    worker_id,
                    url_retry_counts,
                    mq_channel,
                    mq_lock,
                    redis_client,
                    queue_name
                )
                futures.append(future)

            # Wait for workers with timeout check
            for future in futures:
                try:
                    # Wait with timeout to allow checking shutdown
                    while not future.done():
                        if shutdown_event.is_set():
                            break
                        time.sleep(0.5)
                    
                    if not shutdown_event.is_set():
                        future.result(timeout=1)
                        
                except Exception as e:
                    if not shutdown_event.is_set():
                        logger.error(f"Worker failed with exception: {e}")
    
    except KeyboardInterrupt:
        pass  # Already handled by signal handler
    
    finally:
        # Ensure all browsers are closed
        close_all_browsers()

    elapsed = time.time() - start_time

    try:
        mq_connection.close()
        logger.info("✅ RabbitMQ connection closed")
    except Exception as e:
        logger.debug(f"Error closing RabbitMQ: {e}")

    logger.info("=" * 60)
    logger.info("PROCESSING COMPLETE" if not shutdown_event.is_set() else "PROCESSING STOPPED")
    logger.info("=" * 60)
    logger.info(f"Time elapsed:         {elapsed:.2f}s ({elapsed/60:.1f} min)")
    logger.info(f"Workers used:         {MAX_WORKERS}")
    logger.info(f"Consumer ID:          {CONSUMER_ID}")
    logger.info(f"Successfully processed: {total_stats['success']}")
    logger.info(f"Rows written to CSV:  {total_stats['rows_written']}")
    logger.info(f"Offline/Removed:      {total_stats['offline']}")
    logger.info(f"Total errors:         {total_stats['errors']}")
    logger.info(f"Captchas solved:      {total_stats['captchas_solved']}")
    logger.info(f"Browser restarts:     {total_stats['browser_restarts']}")
    logger.info(f"Results saved to:     {CSV_OUTPUT_FILE}")
    logger.info("=" * 60)