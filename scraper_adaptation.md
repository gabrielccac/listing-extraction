# Quick Portal Adaptation Guide

## What to Change

### 1. scraper_worker.py - Configuration (Lines 26-54)

```python
class NewPortalScraper:  # Change class name
    BASE_URL = "https://www.newsite.com.br/"  # Change URL
    SITE_NAME = "newsite"  # Change name

    # Change these selectors by inspecting the website:
    PAGE_LOADED_SELECTOR = 'div.results'
    LISTING_CARD_SELECTOR = 'div.property-card'
    NEXT_PAGE_BUTTON = 'button.next'
    NO_RESULTS_MESSAGE = 'div.no-results'
```

### 2. scraper_worker.py - get_page_url() method (~Line 377)

```python
def get_page_url(self, task, page_num):
    sub_region = task['sub_region']
    transaction = task['transaction_type']

    # Change URL pattern based on website:
    if page_num == 1:
        return f"{self.BASE_URL}/properties-{transaction}-{sub_region}.html"
    else:
        return f"{self.BASE_URL}/properties-{transaction}-{sub_region}-page-{page_num}.html"
```

### 3. scraper_worker.py - extract_page_data() method (~Line 405)

```python
def extract_page_data(self):
    soup = BeautifulSoup(self.sb.get_page_source(), 'lxml')
    listing_elements = soup.select(self.LISTING_CARD_SELECTOR)  # Use your selector

    for listing in listing_elements:
        # Change based on how site stores URLs:
        href = listing.get('href')  # or listing.get('data-url') or listing.select_one('a')['href']

        # Change based on how site shows prices:
        price_element = listing.select_one('span.price')  # Change selector
        price_text = price_element.get_text(strip=True)
        price = int(price_text.replace('R$', '').replace('.', '').strip())
```

### 4. scraper_worker.py - detect_total_pages() method (~Line 550)

```python
def detect_total_pages(self):
    soup = BeautifulSoup(self.sb.get_page_source(), 'lxml')

    # Change based on how site shows total:
    h1 = soup.select_one('h1.title')  # Change selector
    h1_text = h1.get_text(strip=True)

    # Extract number: "1.234 Imóveis" -> 1234
    match = re.search(r'([\d\.]+)\s+Imóveis', h1_text)
    total_listings = int(match.group(1).replace('.', ''))

    # Calculate pages
    LISTINGS_PER_PAGE = 30  # Count manually on website
    return (total_listings + LISTINGS_PER_PAGE - 1) // LISTINGS_PER_PAGE
```

### 5. scraper.py - Only 3 lines (Lines 9, 14, 16)

```python
from scraper_worker import NewPortalScraper  # Line 9 - Change import

SITE_NAME = "newsite"  # Line 14 - Change name
SUB_REGIONS = ["brasilia-df", "rio-rj"]  # Line 16 - Change regions
```

---

## How to Find Selectors

1. Open website in browser
2. Right-click property card → Inspect
3. Note the class/attribute
4. Test in console: `document.querySelectorAll('div.property-card').length`

---

## Comparison of Existing Portals

| What | Imovelweb | DFImoveis | Vivareal |
|------|-----------|-----------|----------|
| **URL Pattern** | `/imoveis-venda-brasilia-pagina-2.html` | `/venda/df/imoveis?pagina=2` | `/venda/brasilia/?pagina=2` |
| **Listing Selector** | `div[data-to-posting]` | `a.imovel-card` | `li[data-cy="property"]` |
| **URL Attribute** | `data-to-posting` | `href` | nested `<a>` tag |
| **Price Selector** | `div.postingPrices-module__price` | `span.body-large.bold` | `div.property-price` |
| **Next Button** | `a[data-qa="PAGING_NEXT"]` | `.btn-outlined.next` | `a[aria-label="próxima página"]` |

---

## Time: ~1 hour total

- 15 min: Find selectors
- 30 min: Update 4 methods
- 15 min: Test

Done.
