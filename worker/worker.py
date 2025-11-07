import time
import logging
import threading
import json
import os
import sys
import signal
import csv
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

# Import from our modules
from queue_client import create_queue_manager, create_redis_deduplicator
from parser import parse_property_data  # NEW: Import parser

# Import pika and redis for compatibility with existing code
import pika
import redis

# Site-specific configuration
SITE_NAME = 'imovelweb'
REDIS_PROCESSED_SET = f'processed_urls_{SITE_NAME}'

# --- Configuration ---

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [Worker-%(thread)d] - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Silence noisy loggers
logging.getLogger("seleniumbase").setLevel(logging.WARNING)
logging.getLogger("undetected_chromedriver").setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)

# Configuration
MAX_RETRIES = 3
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '1'))

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

# Global flag for graceful shutdown
shutdown_event = threading.Event()

# Signal handler for Ctrl+C
def signal_handler(sig, frame):
    logger.info("🚨 Ctrl+C detected! Initiating graceful shutdown...")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)

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
        if sb.is_element_visible('p#Truv1'):
            return True
    except Exception as e:
        logger.debug(f"Error checking for captcha: {e}")
    return False

def handle_captcha(sb, max_attempts=3):
    """Handle captcha if detected"""
    for attempt in range(max_attempts):
        if shutdown_event.is_set():
            logger.info("Shutdown requested, stopping captcha handling")
            return False
        logger.info(f"🤖 Captcha detected! Solving attempt {attempt + 1}/{max_attempts}...")
        try:
            sb.sleep(3)
            if not is_captcha_page(sb):
                logger.info("✓ Captcha solved successfully!")
                with results_lock:
                    total_stats['captchas_solved'] += 1
                return True
            logger.warning(f"Captcha still present after attempt {attempt + 1}")
            sb.sleep(2)
        except Exception as e:
            logger.error(f"Error solving captcha: {str(e)[:100]}")
    logger.error("✗ Failed to solve captcha after all attempts")
    return False

# --- Listing Status Check Function ---
def is_listing_active(soup, url):
    """Check if the listing is still active/available."""
    try:
        offline_div = soup.find('div', class_='section-offline-disclaimer')
        if offline_div:
            offline_text = offline_div.find('p')
            if offline_text and "Este anúncio não está mais publicado" in offline_text.get_text():
                logger.warning(f"Listing is offline: {url}")
                return False
        return True
    except Exception as e:
        logger.error(f"Error checking listing status for {url}: {e}")
        return True

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
        logger.info(f"[{thread_name}] ✅ Browser initialized")

        while not shutdown_event.is_set():
            url = None
            retry_count = 0
            delivery_tag = None

            # Get next URL directly from RabbitMQ using the lock
            try:
                with mq_lock:
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
                logger.error(f"[{thread_name}] ❌ Error fetching from RabbitMQ: {e}")
                time.sleep(5)
                continue

            # Get retry count
            with results_lock:
                retry_count = url_retry_counts.get(url, 0)
                
            # Check if already processed in Redis
            if redis_client.sismember(REDIS_PROCESSED_SET, url):
                logger.info(f"[{thread_name}] ⏭️  Skipping already processed URL: {url}")
                if delivery_tag:
                    with mq_lock:
                        mq_channel.basic_ack(delivery_tag=delivery_tag)
                continue

            # Process this URL
            logger.info(f"[{thread_name}] Processing: {url} (Retry {retry_count})")
            url_start_time = time.time()

            try:
                sb.open(url)

                if is_captcha_page(sb):
                    if not handle_captcha(sb):
                        logger.error(f"[{thread_name}] Failed to solve captcha for {url}")
                        handle_failed_url(url, "Captcha resolution failed",
                                          url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name)

                        # Restart browser after captcha failure
                        try:
                            sb.driver.stop()
                        except:
                            pass

                        try:
                            logger.info(f"[{thread_name}] 🔄 Restarting browser after captcha failure...")
                            sb = sb_cdp.Chrome(uc=True, uc_cdp_events=True, locale="pt-br")
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
                        mq_channel.basic_ack(delivery_tag=delivery_tag)
                    redis_client.srem(REDIS_PROCESSED_SET, url)
                    logger.debug(f"[{thread_name}] Removed offline URL from Redis: {url}")
                    with results_lock:
                        total_stats['offline'] += 1
                    continue

                # NEW: Use parser module instead of inline extraction
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
                        mq_channel.basic_ack(delivery_tag=delivery_tag)
                redis_client.sadd(REDIS_PROCESSED_SET, url)
                
                urls_processed += 1

            except Exception as e:
                logger.error(f"[{thread_name}] ❌ Error processing {url}: {str(e)[:200]}")

                try: 
                    sb.driver.stop()
                except: 
                    pass

                try:
                    logger.info(f"[{thread_name}] 🔄 Restarting browser...")
                    sb = sb_cdp.Chrome(uc=True, uc_cdp_events=True, locale="pt-br")
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
        if sb:
            try:
                logger.info(f"[{thread_name}] 🧹 Closing browser (processed {urls_processed} URLs)")
                sb.driver.stop()
            except Exception as e:
                logger.debug(f"[{thread_name}] Error closing browser: {e}")
        logger.info(f"[{thread_name}] 👋 Worker exiting")

def handle_failed_url(url, error_msg, url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name):
    """Handle a failed URL with retry logic"""
    with results_lock:
        total_stats['errors'] += 1
        retry_count = url_retry_counts.get(url, 0)

        if retry_count < MAX_RETRIES:
            url_retry_counts[url] = retry_count + 1

            with mq_lock:
                if delivery_tag:
                    mq_channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                
                mq_channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=url.encode('utf-8'),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            
            logger.info(f"🔄 Re-queued {url} to RabbitMQ (Retry {retry_count + 1}/{MAX_RETRIES})")
        else:
            logger.error(f"🚫 Max retries reached for {url}, discarding.")
            if delivery_tag:
                with mq_lock:
                    mq_channel.basic_ack(delivery_tag=delivery_tag)

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

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Worker failed with exception: {e}")

    elapsed = time.time() - start_time

    mq_connection.close()
    logger.info("✅ RabbitMQ connection closed")

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