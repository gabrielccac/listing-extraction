import time
import re
import json
import logging
import os
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

logger = logging.getLogger(__name__)


class ImovelwebScraper:
    """
    Scraper for imovelweb.com.br property listings.
    Uses new Redis-only architecture (no RabbitMQ).
    """
    
    # ========================================================================
    # SITE-SPECIFIC CONFIGURATION
    # ========================================================================
    
    BASE_URL = "https://www.imovelweb.com.br/"
    SITE_NAME = "imovelweb"
    
    # Selectors (unchanged)
    PAGE_LOADED_SELECTOR = 'div.postingsList-module__postings-container'
    LISTING_CARD_SELECTOR = 'div.postingCardLayout-module__posting-card-layout[data-to-posting]'
    NEXT_PAGE_BUTTON = 'a.paging-module__page-arrow[data-qa="PAGING_NEXT"]'
    NO_RESULTS_MESSAGE = 'div.postingsNoResults-module__container'
    
    # Captcha detection patterns (unchanged)
    CAPTCHA_TITLE_KEYWORDS = ["Um momento"]
    CAPTCHA_TEXT_KEYWORDS = ["Confirme que você é humano", "cf-challenge"]
    
    # Settings (unchanged)
    LOAD_TIMEOUT = 15
    BROWSER_LOCALE = "pt-br"
    FAILED_DIR = "failed_page_loads"

    # ========================================================================
    # INITIALIZATION - MODIFIED
    # ========================================================================

    def __init__(self, redis_clients: dict):
        """
        Initialize scraper with new Redis clients.
        
        Args:
            redis_clients: Dict with 'scrape_session', 'processed_urls', 'url_stream' clients
        """
        self.scrape_session = redis_clients['scrape_session']
        self.processed_urls = redis_clients['processed_urls']
        self.url_stream = redis_clients['url_stream']
        self.sb = None

        # Ensure failed directory exists
        os.makedirs(self.FAILED_DIR, exist_ok=True)
        
        logger.debug("Scraper initialized with Redis clients")

            # ========================================================================
    # BROWSER MANAGEMENT
    # ========================================================================
    
    def init_browser(self, headless: bool = False):
        """
        Initialize SeleniumBase browser instance.
        
        Args:
            headless: Run in headless mode
        """
        try:
            self.sb = sb_cdp.Chrome(
                uc=True,
                uc_cdp_events=True,
                locale=self.BROWSER_LOCALE,
                headless=headless,
                disable_csp=True,
                disable_images=True
            )
            logger.debug("Browser initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize browser: {str(e)[:100]}")
            raise
    
    def navigate(self, url: str) -> bool:
        """
        Navigate to a URL with captcha detection.

        Workflow:
        1. Navigate to URL
        2. Check for captcha FIRST
        3. Handle captcha if present
        4. Then verify page loaded

        Args:
            url: URL to navigate to

        Returns:
            True if navigation successful (page loaded without captcha or captcha solved)
        """
        try:
            logger.debug(f"Navigating to: {url}")
            self.sb.open(url)

            # Check for captcha BEFORE verifying page load
            if self.is_captcha_page():
                logger.debug("Captcha detected after navigation")
                if not self.handle_captcha():
                    logger.error("Failed to handle captcha during navigation")
                    return False

            # Now verify the actual page loaded
            return self.verify_page_loaded()

        except Exception as e:
            logger.error(f"Navigation failed: {str(e)[:100]}")
            return False
    
    def close_browser(self):
        """Safely close browser instance."""
        try:
            if self.sb:
                self.sb.driver.stop()
                logger.debug("Browser closed")
        except Exception as e:
            logger.debug(f"Error closing browser: {str(e)[:100]}")

    def refresh_page(self) -> bool:
        """
        Soft refresh page (F5 equivalent).

        Returns:
            True if refresh successful
        """
        try:
            logger.debug("Performing soft refresh...")
            self.sb.refresh()
            time.sleep(1)  # Wait for page to stabilize
            return True
        except Exception as e:
            logger.error(f"Soft refresh failed: {str(e)[:100]}")
            return False

    def hard_refresh_page(self) -> bool:
        """
        Hard refresh page with cache clear (Ctrl+Shift+R equivalent).

        Returns:
            True if hard refresh successful
        """
        try:
            logger.debug("Performing hard refresh (cache clear)...")
            # Execute JavaScript to force reload with cache bypass
            self.sb.execute_script("location.reload(true);")
            time.sleep(2)  # Wait longer for cache clear + reload
            return True
        except Exception as e:
            logger.error(f"Hard refresh failed: {str(e)[:100]}")
            return False

    def restart_browser(self) -> bool:
        """
        Close and reinitialize browser (full restart).

        Returns:
            True if restart successful
        """
        try:
            logger.info("Restarting browser...")

            # Close existing browser
            self.close_browser()
            time.sleep(1)

            # Reinitialize browser
            self.init_browser()

            logger.info("Browser restarted successfully")
            return True

        except Exception as e:
            logger.error(f"Browser restart failed: {str(e)[:100]}")
            return False

    # ========================================================================
    # PAGE VERIFICATION
    # ========================================================================
    
    def verify_page_loaded(self) -> bool:
        """
        Verify that the listings page has loaded successfully.

        Returns:
            True if page loaded (listings container visible)
        """
        try:
            self.sb.wait_for_element_visible(
                self.PAGE_LOADED_SELECTOR,
                timeout=self.LOAD_TIMEOUT
            )
            logger.debug("Page loaded successfully")
            return True

        except Exception as e:
            logger.warning(f"Page load verification failed: {str(e)[:100]}")

            # Save screenshot of failed page load
            try:
                timestamp = time.time_ns()
                current_url = self.sb.get_current_url()
                url_hash = current_url.split('/')[-1][:50] if current_url else "unknown"
                screenshot_path = os.path.join(
                    self.FAILED_DIR,
                    f"page_load_failed_{url_hash}_{timestamp}.png"
                )
                self.sb.save_screenshot(screenshot_path)
                logger.info(f"📸 Screenshot saved: {screenshot_path}")
            except Exception as screenshot_error:
                logger.debug(f"Could not save screenshot: {str(screenshot_error)[:100]}")

            return False
    
    # ========================================================================
    # CAPTCHA HANDLING
    # ========================================================================
    
    def is_captcha_page(self) -> bool:
        """
        Detect if current page is a captcha screen.

        Returns:
            True if captcha detected
        """
        try:
            # Check page title
            title = self.sb.get_title()
            for keyword in self.CAPTCHA_TITLE_KEYWORDS:
                if keyword in title:
                    logger.debug(f"Captcha detected in title: '{title}'")
                    return True

            # Check page source
            page_source = self.sb.get_page_source()
            for keyword in self.CAPTCHA_TEXT_KEYWORDS:
                if keyword in page_source:
                    logger.debug(f"Captcha detected in page source: '{keyword}'")
                    return True

        except Exception as e:
            logger.debug(f"Error checking for captcha: {str(e)[:100]}")

        return False

    def is_no_results_page(self) -> bool:
        """
        Detect if current page shows "no results" message.

        This happens when:
        - Legitimately no listings match the filters
        - Server issues prevent reaching calculated max page

        Returns:
            True if no results message detected
        """
        try:
            # Check if no results container is present
            no_results_elements = self.sb.find_elements(self.NO_RESULTS_MESSAGE)

            if no_results_elements:
                logger.debug("No results message detected on page")
                return True

        except Exception as e:
            logger.debug(f"Error checking for no results: {str(e)[:100]}")

        return False
    
    def handle_captcha(self, max_wait: int = 30) -> bool:
        """
        Wait for SeleniumBase UC mode to solve captcha.
        
        Args:
            max_wait: Maximum time to wait (seconds)
            
        Returns:
            True if captcha cleared
        """
        logger.warning("⚠️ Captcha detected! Waiting for UC mode to handle it...")
        
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
    
    # ========================================================================
    # NAVIGATION
    # ========================================================================
    
    def click_next_page(self) -> bool:
        """
        Click the "Next" pagination button.
        
        Returns:
            True if next page clicked successfully
        """
        try:
            if self.sb.assert_element(self.NEXT_PAGE_BUTTON, timeout=15):
                logger.debug("Clicking next page button")
                self.sb.click(self.NEXT_PAGE_BUTTON)
                return True
            else:
                logger.debug("Next button not visible")
                return False
                
        except Exception as e:
            logger.error(f"Error clicking next button: {str(e)[:100]}")
            return False
    
    def get_current_page_number(self) -> int:
        """
        Get current page number from imovelweb URL pattern.
        
        Examples:
            - https://www.imovelweb.com.br/apartamentos-venda-distrito-federal-pagina-509.html
            - https://www.imovelweb.com.br/imoveis-venda-brasilia-df-ordem-publicado-maior-pagina-5.html
        
        Returns:
            Page number (1-indexed), or 1 if not found
        """
        try:
            current_url = self.sb.get_current_url()
            
            # Pattern for imovelweb: -pagina-{number}.html
            match = re.search(r'-pagina-(\d+)\.html', current_url)
            if match:
                return int(match.group(1))
            
            # If no page number found, check if we're on first page (no pagina in URL)
            # First page URLs don't have -pagina- in them
            if '-pagina-' not in current_url:
                return 1
                
        except Exception as e:
            logger.debug(f"Error extracting page number from URL: {str(e)[:100]}")
        
        return 0
    
    # ========================================================================
    # URL CONSTRUCTION
    # ========================================================================
    
    def get_page_url(self, task: dict, page_num: int) -> str:
        """
        Construct URL for a specific page number.

        Args:
            task: Task dict with 'sub_region' and 'transaction_type'
            page_num: Page number (1-indexed)

        Returns:
            Full URL for the page

        Example:
            >>> get_page_url({'sub_region': 'brasilia-df', 'transaction_type': 'venda'}, 5)
            'https://www.imovelweb.com.br/imoveis-venda-brasilia-df-ordem-publicado-maior-pagina-5.html'
        """
        sub_region = task['sub_region']
        transaction_type = task['transaction_type']

        if page_num == 1:
            # First page has different URL format (no -pagina-1)
            return f"{self.BASE_URL}/imoveis-{transaction_type}-{sub_region}-ordem-publicado-maior.html"
        else:
            return f"{self.BASE_URL}/imoveis-{transaction_type}-{sub_region}-ordem-publicado-maior-pagina-{page_num}.html"
    
    # ========================================================================
    # DATA EXTRACTION
    # ========================================================================
    
    def extract_page_data(self) -> list:
        """
        Extract listing URLs with prices from current imovelweb page.
        
        Returns:
            List of tuples: [(url, price), ...]
        """
        try:
            page_html = self.sb.get_page_source()
            soup = BeautifulSoup(page_html, 'lxml')
            
            urls_with_prices = []
            listing_elements = soup.select(self.LISTING_CARD_SELECTOR)
            
            for listing in listing_elements:
                # Extract URL from data-to-posting attribute
                href = listing.get('data-to-posting')
                if not href:
                    continue

                # Build full URL
                if href.startswith('http'):
                    full_url = href
                else:
                    # Remove leading slashes from href to avoid double/triple slashes
                    href_clean = href.lstrip('/')
                    full_url = self.BASE_URL + href_clean

                # NEW: Normalize URL to fix double slashes
                # Fix //propriedades → /propriedades (but keep https://)
                if '//' in full_url:
                    protocol, rest = full_url.split('://', 1)
                    rest = rest.replace('//', '/')
                    full_url = f"{protocol}://{rest}"

                # Clean URL: remove query parameters and anything after .html
                if '?' in full_url:
                    clean_url = full_url.split('?')[0]
                else:
                    clean_url = full_url

                # Ensure it ends with .html (remove any fragments)
                if '.html' in clean_url:
                    clean_url = clean_url.split('.html')[0] + '.html'
                
                # Extract price
                price = None
                
                # Price selector: div with class postingPrices-module__price
                price_div = listing.select_one('div.postingPrices-module__price')
                if price_div:
                    price_text = price_div.get_text(strip=True)
                    # Parse: "R$ 520.000" -> 520000
                    price_clean = price_text.replace('R$', '').replace('.', '').replace(' ', '').strip()
                    try:
                        price = int(price_clean)
                    except (ValueError, AttributeError):
                        logger.debug(f"Could not parse price: {price_text}")
                        pass
                
                # Add tuple
                urls_with_prices.append((clean_url, price))
            
            logger.debug(f"Extracted {len(urls_with_prices)} URLs from page")
            return urls_with_prices
            
        except Exception as e:
            logger.error(f"Error extracting URLs: {str(e)[:100]}")
            return []
        
    # ========================================================================
    # REDIS STORAGE
    # ========================================================================
    
    def store_urls_batch(self, url_price_pairs: list, metadata: dict) -> dict:
        """
        OPTIMIZED: True batch Redis operations with HMGET, pipeline, and HSET mapping
        """
        if not url_price_pairs:
            return {'new': 0, 'price_changes': 0, 'duplicates': 0}

        # Filter out URLs with None prices (Redis doesn't accept None values)
        valid_pairs = [(url, price) for url, price in url_price_pairs if price is not None]
        skipped = len(url_price_pairs) - len(valid_pairs)

        if skipped > 0:
            logger.warning(f"⚠️ Skipped {skipped} URLs with None prices")

        if not valid_pairs:
            return {'new': 0, 'price_changes': 0, 'duplicates': 0}

        stats = {'new': 0, 'price_changes': 0, 'duplicates': 0}
        urls_to_publish = []
        scrape_session_updates = {}
        processed_updates = {}

        # ✅ BATCH 1: Get all existing data in single HMGET operation
        urls = [url for url, _ in valid_pairs]
        existing_data = self.processed_urls.get_urls_batch(urls)

        # Process in batch
        for url, current_price in valid_pairs:
            # Always update scrape_session
            scrape_session_updates[url] = current_price

            historical_data = existing_data.get(url)

            if historical_data:
                historical_price = historical_data.get('price')

                if historical_price == current_price:
                    stats['duplicates'] += 1
                    continue
                else:
                    stats['price_changes'] += 1
                    urls_to_publish.append((url, 'price_update'))
            else:
                stats['new'] += 1
                urls_to_publish.append((url, 'new'))

            # Prepare processed_urls update
            processed_updates[url] = {
                'price': current_price,
                'last_seen': time.time(),
                'first_seen': historical_data.get('first_seen', time.time()) if historical_data else time.time(),
                **metadata
            }

        # ✅ BATCH 2: Update scrape_session in single HSET operation
        if scrape_session_updates:
            self.scrape_session.client.hset(
                self.scrape_session.scrape_session_key,
                mapping=scrape_session_updates
            )

        # ✅ BATCH 3: Publish to stream using pipeline (single round-trip)
        if urls_to_publish:
            self.url_stream.publish_urls_batch(urls_to_publish)

        # ✅ BATCH 4: Update processed_urls in single HSET operation with mapping
        if processed_updates:
            serialized_updates = {url: json.dumps(data) for url, data in processed_updates.items()}
            self.processed_urls.client.hset(
                self.processed_urls.processed_key,
                mapping=serialized_updates
            )

        logger.info(f"📊 Batch: {len(valid_pairs)} URLs → "
                f"🆕{stats['new']} 💰{stats['price_changes']} 🔄{stats['duplicates']}")

        return stats
    
    # ========================================================================
    # TOTAL PAGES DETECTION
    # ========================================================================
    
    def detect_total_pages(self) -> int:
        """
        Detect total number of pages from current page (reuses existing browser).
        
        Reads h1 title to calculate page count.
        
        Returns:
            Total pages (int), or 0 if detection fails
        """
        MAX_PAGES = 555  # Website blocks beyond this
        LISTINGS_PER_PAGE = 30
        
        logger.info(f"🔍 Detecting total pages from current page...")
        
        try:
            # Wait for the page title to load (we're already on the page)
            self.sb.wait_for_element_visible('h1.postingsTitle-module__title', timeout=15)
            page_html = self.sb.get_page_source()
            soup = BeautifulSoup(page_html, 'lxml')
            
            # Find the h1 element with total listings
            h1_element = soup.select_one('h1.postingsTitle-module__title')
            if not h1_element:
                logger.warning("No title found for page detection")
                return 0
            
            h1_text = h1_element.get_text(strip=True)
            logger.debug(f"Found title: {h1_text}")
            
            # Extract total listings number from h1 text
            pattern = r'([\d\.]+)\s+(?:Imóveis|Apartamentos|Casas|Terrenos)'
            match = re.search(pattern, h1_text)
            
            if not match:
                logger.warning(f"Could not extract total listings from: {h1_text}")
                return 0
            
            # Extract number, remove dots
            total_listings_str = match.group(1).replace('.', '')
            total_listings = int(total_listings_str)
            
            # Calculate pages (round up)
            expected_pages = (total_listings + LISTINGS_PER_PAGE - 1) // LISTINGS_PER_PAGE
            
            # Apply max pages limit
            total_pages = min(expected_pages, MAX_PAGES)
            
            logger.info(f"✅ Detected {total_pages} pages ({total_listings} listings)")
            return total_pages
                
        except Exception as e:
            logger.error(f"Error detecting pages: {str(e)[:200]}")
            return 0
        
        