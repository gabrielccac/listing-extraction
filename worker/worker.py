import time
import logging
import threading
import json
import os
import sys
import re
import signal
import csv
# from collections import deque # No longer needed
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

# Add parent directory to path for utils import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from queue_client import create_queue_manager, create_redis_deduplicator

# Import pika and redis for compatibility with existing code
import pika
import redis

# Site-specific configuration
SITE_NAME = 'imovelweb'
REDIS_PROCESSED_SET = f'processed_urls_{SITE_NAME}'  # Redis set for processed URLs

# --- Configuration ---

# Configure logging - thread-safe
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [Worker-%(thread)d] - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Add this block to silence noisy loggers ---
logging.getLogger("seleniumbase").setLevel(logging.WARNING)
logging.getLogger("undetected_chromedriver").setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)

# Configuration
MAX_RETRIES = 3
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '1'))  # Max parallel browsers (configurable via env var)
# BATCH_SIZE = 10 # No longer needed

# CSV Output file - unique per consumer instance
CONSUMER_ID = os.getpid()  # Use process ID as unique identifier
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '.')  # Use /app/output in Docker, current dir otherwise
CSV_OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'imovelweb_listings_{CONSUMER_ID}.csv')
FAILED_DIR = "failed_urls"

# Ensure directories exist
os.makedirs(FAILED_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Thread-safe storage & stats
results_lock = threading.Lock()
mq_lock = threading.Lock() # ADDED: Lock for thread-safe RabbitMQ operations
total_stats = {
    'success': 0,
    'errors': 0,
    'captchas_solved': 0,
    'browser_restarts': 0,
    'offline': 0, # Stat for offline listings
    'rows_written': 0, # Total rows written to CSV
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

    # Define the fieldnames based on the schema
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
        # --- FIX 2: Added results_lock to make CSV writing thread-safe ---
        with results_lock:
            with open(CSV_OUTPUT_FILE, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header if file is new
                if not file_exists:
                    writer.writeheader()

                # Ensure all fields exist in property_data
                row = {field: property_data.get(field, "") for field in fieldnames}

                # Convert JSON strings back to regular strings for CSV
                if row.get("contatos_adicionais"):
                    try:
                        contatos = json.loads(row["contatos_adicionais"])
                        row["contatos_adicionais"] = ", ".join(contatos) if isinstance(contatos, list) else row["contatos_adicionais"]
                    except:
                        pass

                if row.get("carousel_img_links"):
                    try:
                        carousel = json.loads(row["carousel_img_links"])
                        row["carousel_img_links"] = ", ".join(carousel) if isinstance(carousel, list) else row["carousel_img_links"]
                    except:
                        pass

                if row.get("features"):
                    try:
                        features = json.loads(row["features"])
                        row["features"] = ", ".join(features) if isinstance(features, list) else row["features"]
                    except:
                        pass

                writer.writerow(row)
        # --- End of FIX 2 ---

        # Increment row counter
        total_stats['rows_written'] += 1
        rows_written = total_stats['rows_written']

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
    """
    Check if the listing is still active/available by looking for the
    offline disclaimer section.
    """
    try:
        offline_div = soup.find('div', class_='section-offline-disclaimer')
        if offline_div:
            offline_text = offline_div.find('p')
            if offline_text and "Este anúncio não está mais publicado" in offline_text.get_text():
                logger.warning(f"Listing is offline: {url}")
                return False # Listing is NOT active
        return True # Listing is active
    except Exception as e:
        logger.error(f"Error checking listing status for {url}: {e}")
        return True # Default to active

# --- Extraction Logic ---
def extract_aviso_info(html):
    """Extract avisoInfo object from HTML"""
    regex_inicio = r'\{\s*([\"]?units[\"]?|[\"]?similarLink[\"]?|[\"]?urlBack[\"]?)'
    match = re.search(regex_inicio, html)
    if not match: return None
    start_pos = match.start()
    open_braces = 0
    end_pos = start_pos
    for i in range(start_pos, len(html)):
        if html[i] == '{': open_braces += 1
        elif html[i] == '}':
            open_braces -= 1
            if open_braces == 0: end_pos = i + 1; break
    if open_braces != 0: return None
    objeto_str = html[start_pos:end_pos]
    try:
        objeto_corrigido = re.sub(r'urlMapOf|mapLatOf|mapLngOf', 'null', objeto_str)
        namespace = {'null': None, 'false': False, 'true': True}
        return eval(objeto_corrigido, namespace)
    except Exception:
        try:
            json_str = objeto_str
            json_str = re.sub(r'([{,]\s*)([a-zA-Z0-9_$]+)(\s*:)', r'\1"\2"\3', json_str)
            json_str = re.sub(r"'", '"', json_str)
            json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
            json_str = re.sub(r'\b(false|true|null)\b', lambda m: m.group(1).lower(), json_str)
            json_str = re.sub(r'urlMapOf|mapLatOf|mapLngOf', 'null', json_str)
            json_str = re.sub(r'"true"', 'true', json_str)
            json_str = re.sub(r'"false"', 'false', json_str)
            return json.loads(json_str)
        except Exception:
            return None

def extract_property_data(soup, url, html):
    """Extract property data from the page"""
    property_data = {}
    aviso_info = extract_aviso_info(html)

    # Group 1: Basic Info
    property_data["url_imovel"] = url
    property_data["_source"] = "imovelweb"
    property_data["short_id"] = ""
    if aviso_info and aviso_info.get('realEstateType') and aviso_info['realEstateType'].get('name'):
        property_data["tipo_imovel"] = aviso_info['realEstateType']['name']
    else: property_data["tipo_imovel"] = ""
    property_data["enquadramento"] = ""
    if aviso_info and aviso_info.get('pricesData') and len(aviso_info['pricesData']) > 0:
        operation_type = aviso_info['pricesData'][0].get('operationType')
        if operation_type and operation_type.get('name'):
            property_data["purpose_type"] = operation_type['name']
        else: property_data["purpose_type"] = ""
    else: property_data["purpose_type"] = ""
    if aviso_info and aviso_info.get('publicationDateFormatted'):
        property_data["dt_atualizacao"] = aviso_info['publicationDateFormatted']
    else: property_data["dt_atualizacao"] = ""
    if aviso_info and aviso_info.get('mainFeatures') and aviso_info['mainFeatures'].get('CFT5'):
        cft5_value = aviso_info['mainFeatures']['CFT5'].get('value')
        if cft5_value:
            property_data["stage_description"] = "Em construção" if cft5_value == "Em construção" else "Pronto"
        else: property_data["stage_description"] = ""
    else: property_data["stage_description"] = ""
    title_tag = soup.find('title')
    property_data["titulo"] = title_tag.get_text(strip=True) if title_tag else ""
    if aviso_info and aviso_info.get('description'):
        descricao_text = aviso_info['description'].replace('<br>', ' ').replace('<br/>', ' ')
        descricao_clean = re.sub(r'<[^>]+>', '', descricao_text)
        property_data["descricao"] = re.sub(r'\s+', ' ', descricao_clean).strip()
    else: property_data["descricao"] = ""

    # Group 2: Location
    endereco_parts = []
    if aviso_info and aviso_info.get('address') and aviso_info['address'].get('name'):
        endereco_parts.append(aviso_info['address']['name'])
    if aviso_info and aviso_info.get('location'):
        location = aviso_info['location']
        if location.get('name'):
            endereco_parts.append(location['name'])
        if location.get('parent') and location['parent'].get('name'):
            endereco_parts.append(location['parent']['name'])
    property_data["endereco"] = ", ".join(endereco_parts) if endereco_parts else ""
    if aviso_info and aviso_info.get('location') and aviso_info['location'].get('name'):
        property_data["bairro"] = aviso_info['location']['name']
    else: property_data["bairro"] = ""
    if aviso_info and aviso_info.get('location') and aviso_info['location'].get('parent'):
        parent = aviso_info['location']['parent']
        property_data["cidade"] = parent['name'] if parent.get('name') else ""
    else: property_data["cidade"] = ""

    # Group 3: Advertiser
    publisher_name = ""
    if aviso_info and aviso_info.get('publisher') and aviso_info['publisher'].get('name'):
        publisher_name = aviso_info['publisher']['name']
    property_data["anunciante"] = publisher_name
    property_data["responsavel"] = publisher_name
    if aviso_info and aviso_info.get('publisher') and aviso_info['publisher'].get('license'):
        property_data["anunciante_creci"] = aviso_info['publisher']['license']
    else: property_data["anunciante_creci"] = ""
    if aviso_info and aviso_info.get('publisher') and aviso_info['publisher'].get('publisherTypeId'):
        publisher_type_id = aviso_info['publisher']['publisherTypeId']
        if publisher_type_id == 2: property_data["tipo_responsavel"] = "Imobiliária"
        elif publisher_type_id == 1: property_data["tipo_responsavel"] = "Proprietário"
        else: property_data["tipo_responsavel"] = ""
    else: property_data["tipo_responsavel"] = ""
    if aviso_info and aviso_info.get('whatsApp'):
        property_data["contato_responsavel"] = aviso_info['whatsApp']
    else: property_data["contato_responsavel"] = ""

    # Group 4: Features
    def get_feature_value(features, key, default=None, value_type=float):
        if features and features.get(key):
            try: return value_type(features[key].get('value', default))
            except (ValueError, TypeError): return default
        return default
    
    main_features = aviso_info.get('mainFeatures') if aviso_info else None
    property_data["metragem"] = get_feature_value(main_features, 'CFT100')
    property_data["quartos"] = get_feature_value(main_features, 'CFT2', value_type=int)
    property_data["suites"] = get_feature_value(main_features, 'CFT4', value_type=int)
    property_data["banheiro"] = get_feature_value(main_features, 'CFT3', value_type=int)
    property_data["vagas"] = get_feature_value(main_features, 'CFT7', value_type=int)

    # Group 5: Pricing
    property_data["valor"] = None
    if aviso_info and aviso_info.get('pricesData') and len(aviso_info['pricesData']) > 0:
        prices = aviso_info['pricesData'][0].get('prices', [])
        for price in prices:
            if price.get('isoCode') == 'BRL':
                try: property_data["valor"] = float(price.get('amount')) if price.get('amount') else None; break
                except (ValueError, TypeError): pass
    try:
        property_data["condominio"] = float(aviso_info['expenses']) if aviso_info and aviso_info.get('expenses') else None
    except (ValueError, TypeError): property_data["condominio"] = None
    property_data["iptu"] = None
    price_expenses_span = soup.find('span', class_='price-expenses')
    if price_expenses_span:
        iptu_match = re.search(r'IPTU\s+R\$\s*([\d,.]+)', price_expenses_span.get_text())
        if iptu_match:
            try: property_data["iptu"] = float(iptu_match.group(1).replace('.', '').replace(',', '.'))
            except (ValueError, TypeError): pass

    # Group 6: Images
    if aviso_info and aviso_info.get('pictures') and len(aviso_info['pictures']) > 0:
        property_data["picture_link"] = aviso_info['pictures'][0].get('url730x532', '')
        carousel_images = [img.get('url730x532', '') for img in aviso_info['pictures'][1:4] if img.get('url730x532')]
        property_data["carousel_img_links"] = ", ".join(carousel_images) if carousel_images else ""
    else:
        property_data["picture_link"] = ""
        property_data["carousel_img_links"] = ""

    # Group 7: Details & Amenities
    property_data["features"] = ""
    if property_data.get("valor") and property_data.get("metragem") and property_data["metragem"] > 0:
        try: property_data["preco_m2"] = round(property_data["valor"] / property_data["metragem"], 2)
        except (ValueError, TypeError, ZeroDivisionError): property_data["preco_m2"] = None
    else: property_data["preco_m2"] = None

    general_features = aviso_info.get('generalFeatures') if aviso_info else {}
    outros_features = general_features.get('Outros', {})
    property_data["andar"] = str(outros_features.get('30001', {}).get('value', ''))
    property_data["piso"] = ""
    for features in general_features.values():
        if features.get('20148'):
            property_data["piso"] = features['20148'].get('label', ''); break
    property_data["posicao_solar"] = str(outros_features.get('3005', {}).get('value', ''))
    
    property_data["mobiliado"] = None; property_data["reformado"] = None; property_data["reforma_hidraulica"] = None
    property_data["reforma_eletrica"] = None; property_data["fachada_reformada"] = None; property_data["dce"] = None
    property_data["vista_livre"] = None; property_data["vazado"] = None; property_data["piscina_aquecida"] = None
    property_data["pista_skate"] = None

    def check_feature(feature_id):
        for features in general_features.values():
            if features.get(feature_id): return True
        return None

    property_data["elevador"] = check_feature('10071') or check_feature('20071')
    property_data["varanda"] = check_feature('20199')
    property_data["gas_encanado"] = check_feature('20099')
    cozinha_map = {'20056': 'Cozinha americana', '20057': 'Cozinha Gourmet', '20058': 'Cozinha independente'}
    cozinha_types = [cozinha_map[fid] for fid in cozinha_map if check_feature(fid)]
    property_data["tipo_cozinha"] = ", ".join(cozinha_types)
    property_data["aceita_permuta"] = check_feature('10088')
    property_data["aceita_fgts"] = '30002' in outros_features
    property_data["aceita_financiamento"] = check_feature('20003')

    # Leisure
    property_data["sala_ginastica"] = check_feature('10090')
    property_data["salao_de_festas"] = check_feature('10181')
    property_data["salao_de_jogos"] = check_feature('10182')
    property_data["sauna"] = check_feature('10183')
    property_data["piscina"] = check_feature('10140')
    property_data["quadra_esportiva"] = check_feature('10165')
    property_data["churrasqueira"] = check_feature('10048')
    property_data["cinema"] = check_feature('20049')
    property_data["lavanderia"] = check_feature('20117')
    property_data["playground"] = check_feature('10152')

    # Group 8: Regex from Description
    description_lower = property_data.get("descricao", "").lower()
    if re.search(r'\bmobili(a|ado|ada)\b', description_lower): property_data["mobiliado"] = True
    if re.search(r'\breform(a|ado|ada)\b', description_lower): property_data["reformado"] = True
    if re.search(r'\bvista(\s+livre)?\b', description_lower): property_data["vista_livre"] = True
    if re.search(r'\bvazado\b', description_lower): property_data["vazado"] = True
    if re.search(r'\b(piscina\s+aquecida|água\s+aquecida)\b', description_lower): property_data["piscina_aquecida"] = True
    if re.search(r'\b(skate|pista\s+(de\s+)?skate)\b', description_lower): property_data["pista_skate"] = True

    # Group 9: Leisure Calculation
    leisure_fields = ["sala_ginastica", "salao_de_festas", "piscina", "sauna", "salao_de_jogos", 
                        "quadra_esportiva", "cinema", "playground", "churrasqueira"]
    leisure_count = sum(1 for field in leisure_fields if property_data.get(field))
    property_data["lazer_completo"] = leisure_count >= 3
    property_data["lazer_parcial"] = 1 <= leisure_count < 3

    # Group 10: IDs
    property_data["anunciante_id"] = str(aviso_info.get('postingCode', '')) if aviso_info else ""
    property_data["anuncio_id"] = str(aviso_info.get('idAviso', '')) if aviso_info else ""
    
    return property_data


# --- Worker Function ---
# --- FIX 1: Reverted to "Direct Consumer" architecture ---
# MODIFIED: Signature changed to accept mq_lock and remove url_queue
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
        # wait for browser to be ready
        logger.info(f"[{thread_name}] ✅ Browser initialized")

        while not shutdown_event.is_set():
            url = None
            retry_count = 0
            delivery_tag = None

            # MODIFIED: Get next URL directly from RabbitMQ using the lock
            try:
                with mq_lock:
                    method_frame, header_frame, body = mq_channel.basic_get(
                        queue=queue_name, auto_ack=False
                    )
                
                if method_frame:
                    url = body.decode('utf-8')
                    delivery_tag = method_frame.delivery_tag
                else:
                    # --- MUDANÇA AQUI ---
                    # Fila está vazia, então o worker deve parar.
                    logger.info(f"[{thread_name}] 📭 Fila vazia. Encerrando worker.")
                    break # <-- SAI DO LOOP 'while'
                    # --- FIM DA MUDANÇA ---
            
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
                    with mq_lock: # MODIFIED: Use lock
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
                        # MODIFIED: Pass mq_lock, remove url_queue
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
                            break  # Exit worker if browser restart fails

                        continue

                html = sb.get_page_source()
                soup = BeautifulSoup(html, 'lxml')

                if not is_listing_active(soup, url):
                    logger.warning(f"[{thread_name}] 🛑 Listing is OFFLINE, skipping: {url}")
                    with mq_lock:
                        mq_channel.basic_ack(delivery_tag=delivery_tag)
                    # Remove from Redis so scraper can re-check it in future scrapes
                    redis_client.srem(REDIS_PROCESSED_SET, url)
                    logger.debug(f"[{thread_name}] Removed offline URL from Redis: {url}")
                    with results_lock:
                        total_stats['offline'] += 1
                    continue

                aviso_info = extract_aviso_info(html)
                if not aviso_info:
                    logger.warning(f"[{thread_name}] 'aviso_info' object not found in HTML for {url}")
                    sb.save_screenshot(os.path.join(FAILED_DIR, f"no_aviso_{time.time_ns()}.png"))
                    # MODIFIED: Pass mq_lock, remove url_queue
                    handle_failed_url(url, "No aviso_info found",
                                      url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name)
                    continue

                property_data = extract_property_data(soup, url, html)

                if not property_data or not property_data.get("titulo"):
                    logger.warning(f"[{thread_name}] No valid data found for {url}")
                    sb.save_screenshot(os.path.join(FAILED_DIR, f"no_data_{time.time_ns()}.png"))
                    # MODIFIED: Pass mq_lock, remove url_queue
                    handle_failed_url(url, "No valid data extracted",
                                      url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name)
                    continue

                if not save_to_csv(property_data):
                    logger.error(f"[{thread_name}] Failed to save to CSV for {url}")
                    # MODIFIED: Pass mq_lock, remove url_queue
                    handle_failed_url(url, "CSV save failed",
                                      url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name)
                    continue

                # Success!
                url_elapsed = time.time() - url_start_time
                logger.info(f"[{thread_name}] ✅ Success for {url} in {url_elapsed:.2f}s")
                
                with results_lock:
                    total_stats['success'] += 1
                
                if delivery_tag:
                    with mq_lock: # MODIFIED: Use lock
                        mq_channel.basic_ack(delivery_tag=delivery_tag)
                redis_client.sadd(REDIS_PROCESSED_SET, url)
                
                urls_processed += 1

            except Exception as e:
                logger.error(f"[{thread_name}] ❌ Error processing {url}: {str(e)[:200]}")

                try: sb.driver.stop()
                except: pass

                try:
                    logger.info(f"[{thread_name}] 🔄 Restarting browser...")
                    sb = sb_cdp.Chrome(uc=True, uc_cdp_events=True, locale="pt-br")
                    with results_lock:
                        total_stats['browser_restarts'] += 1
                    logger.info(f"[{thread_name}] ✅ Browser restarted successfully")
                except Exception as restart_error:
                    logger.error(f"[{thread_name}] 🚨 Failed to restart browser: {restart_error}")
                    if url and delivery_tag:
                        # MODIFIED: Pass mq_lock, remove url_queue
                        handle_failed_url(url, str(e), url_retry_counts,
                                          mq_channel, mq_lock, delivery_tag, queue_name)
                    break 

                # Re-queue the URL
                # MODIFIED: Pass mq_lock, remove url_queue
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

# MODIFIED: Signature changed to accept mq_lock and remove url_queue
def handle_failed_url(url, error_msg, url_retry_counts, mq_channel, mq_lock, delivery_tag, queue_name):
    """Handle a failed URL with retry logic"""
    with results_lock:
        total_stats['errors'] += 1
        retry_count = url_retry_counts.get(url, 0)

        if retry_count < MAX_RETRIES:
            url_retry_counts[url] = retry_count + 1

            # Nack and requeue to RabbitMQ
            with mq_lock: # MODIFIED: Use lock
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
                with mq_lock: # MODIFIED: Use lock
                    mq_channel.basic_ack(delivery_tag=delivery_tag)


# --- Main Thread (Conductor) ---

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

    # MODIFIED: local queue (deque) removed
    url_retry_counts = {}
    queue_name = queue_mgr.queue_name

    logger.info("=" * 60)
    logger.info(f"Starting ImovelWeb Persistent Session Processor (Direct Consumer Arch)")
    logger.info(f"Consumer ID: {CONSUMER_ID} | Max Workers: {MAX_WORKERS}")
    logger.info(f"Queue: {queue_name}")
    logger.info(f"Output file: {CSV_OUTPUT_FILE}")
    logger.info("=" * 60)

    # MODIFIED: Queue feeder thread removed
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for worker_id in range(MAX_WORKERS):
            # MODIFIED: Pass mq_lock, remove urls_to_process (deque)
            future = executor.submit(
                persistent_worker,
                worker_id,
                url_retry_counts,
                mq_channel,
                mq_lock, # ADDED
                redis_client,
                queue_name
            )
            futures.append(future)

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Worker failed with exception: {e}")

    # --- End of Script ---
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