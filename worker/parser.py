"""
DFImoveis Property Parser (Single-File Structure)

Contains all logic, configuration, and testing for parsing dfimoveis pages.
Reads 'page.html' in the same directory when run as a script.
"""

import re
import json
import logging
import datetime
from bs4 import BeautifulSoup, Tag


def extract_aviso_info(html: str) -> dict:
    """
    Extract avisoInfo JavaScript object from HTML.

    This is specific to imovelweb - they embed property data in a JS object.

    Args:
        html: Page HTML source

    Returns:
        Dict containing aviso_info data, or None if not found
    """
    # Find the start of the avisoInfo object
    regex_inicio = r'\{\s*([\"]?units[\"]?|[\"]?similarLink[\"]?|[\"]?urlBack[\"]?)'
    match = re.search(regex_inicio, html)

    if not match:
        return None

    start_pos = match.start()
    open_braces = 0
    end_pos = start_pos

    # Find the matching closing brace
    for i in range(start_pos, len(html)):
        if html[i] == '{':
            open_braces += 1
        elif html[i] == '}':
            open_braces -= 1
            if open_braces == 0:
                end_pos = i + 1
                break

    if open_braces != 0:
        return None

    objeto_str = html[start_pos:end_pos]

    # Try to parse as Python eval first
    try:
        objeto_corrigido = re.sub(r'urlMapOf|mapLatOf|mapLngOf', 'null', objeto_str)
        namespace = {'null': None, 'false': False, 'true': True}
        return eval(objeto_corrigido, namespace)
    except Exception:
        pass

    # Try to parse as JSON
    try:
        json_str = objeto_str
        # Quote unquoted keys
        json_str = re.sub(r'([{,]\s*)([a-zA-Z0-9_$]+)(\s*:)', r'\1"\2"\3', json_str)
        # Convert single quotes to double quotes
        json_str = re.sub(r"'", '"', json_str)
        # Remove trailing commas
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        # Lowercase boolean/null values
        json_str = re.sub(r'\b(false|true|null)\b', lambda m: m.group(1).lower(), json_str)
        # Replace function references with null
        json_str = re.sub(r'urlMapOf|mapLatOf|mapLngOf', 'null', json_str)
        # Fix quoted booleans
        json_str = re.sub(r'"true"', 'true', json_str)
        json_str = re.sub(r'"false"', 'false', json_str)

        return json.loads(json_str)
    except Exception:
        return None


def parse_property_data(soup: BeautifulSoup, url: str, html: str) -> dict:
    """
    Extract all property data from imovelweb listing page.

    This is the main extraction function for imovelweb.
    For other sites, this function will be different.

    Args:
        soup: BeautifulSoup object of the page
        url: Property URL
        html: Raw HTML source

    Returns:
        Dict containing all extracted property fields
    """
    property_data = {}
    aviso_info = extract_aviso_info(html)

    # ========================================================================
    # GROUP 1: BASIC INFO
    # ========================================================================

    property_data["url_imovel"] = url
    property_data["_source"] = "imovelweb"
    property_data["short_id"] = ""

    # Property type
    if aviso_info and aviso_info.get('realEstateType') and aviso_info['realEstateType'].get('name'):
        property_data["tipo_imovel"] = aviso_info['realEstateType']['name']
    else:
        property_data["tipo_imovel"] = ""

    property_data["enquadramento"] = ""

    # Purpose type (Venda/Aluguel)
    if aviso_info and aviso_info.get('pricesData') and len(aviso_info['pricesData']) > 0:
        operation_type = aviso_info['pricesData'][0].get('operationType')
        if operation_type and operation_type.get('name'):
            property_data["purpose_type"] = operation_type['name']
        else:
            property_data["purpose_type"] = ""
    else:
        property_data["purpose_type"] = ""

    # Update date
    if aviso_info and aviso_info.get('publicationDateFormatted'):
        property_data["dt_atualizacao"] = aviso_info['publicationDateFormatted']
    else:
        property_data["dt_atualizacao"] = ""

    # Stage (Em construção / Pronto)
    if aviso_info and aviso_info.get('mainFeatures') and aviso_info['mainFeatures'].get('CFT5'):
        cft5_value = aviso_info['mainFeatures']['CFT5'].get('value')
        if cft5_value:
            property_data["stage_description"] = "Em construção" if cft5_value == "Em construção" else "Pronto"
        else:
            property_data["stage_description"] = ""
    else:
        property_data["stage_description"] = ""

    # Title
    title_tag = soup.find('title')
    property_data["titulo"] = title_tag.get_text(strip=True) if title_tag else ""

    # Description
    if aviso_info and aviso_info.get('description'):
        descricao_text = aviso_info['description'].replace('<br>', ' ').replace('<br/>', ' ')
        descricao_clean = re.sub(r'<[^>]+>', '', descricao_text)
        property_data["descricao"] = re.sub(r'\s+', ' ', descricao_clean).strip()
    else:
        property_data["descricao"] = ""

    # ========================================================================
    # GROUP 2: LOCATION
    # ========================================================================

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
    else:
        property_data["bairro"] = ""

    if aviso_info and aviso_info.get('location') and aviso_info['location'].get('parent'):
        parent = aviso_info['location']['parent']
        property_data["cidade"] = parent['name'] if parent.get('name') else ""
    else:
        property_data["cidade"] = ""

    # ========================================================================
    # GROUP 3: ADVERTISER
    # ========================================================================

    publisher_name = ""
    if aviso_info and aviso_info.get('publisher') and aviso_info['publisher'].get('name'):
        publisher_name = aviso_info['publisher']['name']

    property_data["anunciante"] = publisher_name
    property_data["responsavel"] = publisher_name

    if aviso_info and aviso_info.get('publisher') and aviso_info['publisher'].get('license'):
        property_data["anunciante_creci"] = aviso_info['publisher']['license']
    else:
        property_data["anunciante_creci"] = ""

    if aviso_info and aviso_info.get('publisher') and aviso_info['publisher'].get('publisherTypeId'):
        publisher_type_id = aviso_info['publisher']['publisherTypeId']
        if publisher_type_id == 2:
            property_data["tipo_responsavel"] = "Imobiliária"
        elif publisher_type_id == 1:
            property_data["tipo_responsavel"] = "Proprietário"
        else:
            property_data["tipo_responsavel"] = ""
    else:
        property_data["tipo_responsavel"] = ""

    if aviso_info and aviso_info.get('whatsApp'):
        property_data["contato_responsavel"] = aviso_info['whatsApp']
    else:
        property_data["contato_responsavel"] = ""

    property_data["contatos_adicionais"] = ""

    # ========================================================================
    # GROUP 4: FEATURES
    # ========================================================================

    def get_feature_value(features, key, default=None, value_type=float):
        """Helper to extract feature values"""
        if features and features.get(key):
            try:
                return value_type(features[key].get('value', default))
            except (ValueError, TypeError):
                return default
        return default

    main_features = aviso_info.get('mainFeatures') if aviso_info else None
    property_data["metragem"] = get_feature_value(main_features, 'CFT100')
    property_data["quartos"] = get_feature_value(main_features, 'CFT2', value_type=int)
    property_data["suites"] = get_feature_value(main_features, 'CFT4', value_type=int)
    property_data["banheiro"] = get_feature_value(main_features, 'CFT3', value_type=int)
    property_data["vagas"] = get_feature_value(main_features, 'CFT7', value_type=int)

    # ========================================================================
    # GROUP 5: PRICING
    # ========================================================================

    property_data["valor"] = None
    if aviso_info and aviso_info.get('pricesData') and len(aviso_info['pricesData']) > 0:
        prices = aviso_info['pricesData'][0].get('prices', [])
        for price in prices:
            if price.get('isoCode') == 'BRL':
                try:
                    property_data["valor"] = float(price.get('amount')) if price.get('amount') else None
                    break
                except (ValueError, TypeError):
                    pass

    try:
        property_data["condominio"] = float(aviso_info['expenses']) if aviso_info and aviso_info.get('expenses') else None
    except (ValueError, TypeError):
        property_data["condominio"] = None

    property_data["iptu"] = None
    price_expenses_span = soup.find('span', class_='price-expenses')
    if price_expenses_span:
        iptu_match = re.search(r'IPTU\s+R\$\s*([\d,.]+)', price_expenses_span.get_text())
        if iptu_match:
            try:
                property_data["iptu"] = float(iptu_match.group(1).replace('.', '').replace(',', '.'))
            except (ValueError, TypeError):
                pass

    # Price per m²
    if property_data.get("valor") and property_data.get("metragem") and property_data["metragem"] > 0:
        try:
            property_data["preco_m2"] = round(property_data["valor"] / property_data["metragem"], 2)
        except (ValueError, TypeError, ZeroDivisionError):
            property_data["preco_m2"] = None
    else:
        property_data["preco_m2"] = None

    # ========================================================================
    # GROUP 6: IMAGES
    # ========================================================================

    if aviso_info and aviso_info.get('pictures') and len(aviso_info['pictures']) > 0:
        property_data["picture_link"] = aviso_info['pictures'][0].get('url730x532', '')
        carousel_images = [img.get('url730x532', '') for img in aviso_info['pictures'][1:4] if img.get('url730x532')]
        property_data["carousel_img_links"] = ", ".join(carousel_images) if carousel_images else ""
    else:
        property_data["picture_link"] = ""
        property_data["carousel_img_links"] = ""

    # ========================================================================
    # GROUP 7: DETAILS & AMENITIES
    # ========================================================================

    general_features = aviso_info.get('generalFeatures') if aviso_info else {}
    outros_features = general_features.get('Outros', {})

    property_data["andar"] = str(outros_features.get('30001', {}).get('value', ''))

    property_data["piso"] = ""
    for features in general_features.values():
        if features.get('20148'):
            property_data["piso"] = features['20148'].get('label', '')
            break

    property_data["posicao_solar"] = str(outros_features.get('3005', {}).get('value', ''))

    # Initialize boolean fields
    property_data["mobiliado"] = None
    property_data["reformado"] = None
    property_data["reforma_hidraulica"] = None
    property_data["reforma_eletrica"] = None
    property_data["fachada_reformada"] = None
    property_data["dce"] = None
    property_data["vista_livre"] = None
    property_data["vazado"] = None
    property_data["piscina_aquecida"] = None
    property_data["pista_skate"] = None

    def check_feature(feature_id):
        """Helper to check if feature exists"""
        for features in general_features.values():
            if features.get(feature_id):
                return True
        return None

    property_data["elevador"] = check_feature('10071') or check_feature('20071')
    property_data["varanda"] = check_feature('20199')
    property_data["gas_encanado"] = check_feature('20099')

    # Kitchen types
    cozinha_map = {
        '20056': 'Cozinha americana',
        '20057': 'Cozinha Gourmet',
        '20058': 'Cozinha independente'
    }
    cozinha_types = [cozinha_map[fid] for fid in cozinha_map if check_feature(fid)]
    property_data["tipo_cozinha"] = ", ".join(cozinha_types)

    # Payment options
    property_data["aceita_permuta"] = check_feature('10088')
    property_data["aceita_fgts"] = '30002' in outros_features
    property_data["aceita_financiamento"] = check_feature('20003')

    # Leisure amenities
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

    # ========================================================================
    # GROUP 8: REGEX FROM DESCRIPTION
    # ========================================================================

    description_lower = property_data.get("descricao", "").lower()
    if re.search(r'\bmobili(a|ado|ada)\b', description_lower):
        property_data["mobiliado"] = True
    if re.search(r'\breform(a|ado|ada)\b', description_lower):
        property_data["reformado"] = True
    if re.search(r'\bvista(\s+livre)?\b', description_lower):
        property_data["vista_livre"] = True
    if re.search(r'\bvazado\b', description_lower):
        property_data["vazado"] = True
    if re.search(r'\b(piscina\s+aquecida|água\s+aquecida)\b', description_lower):
        property_data["piscina_aquecida"] = True
    if re.search(r'\b(skate|pista\s+(de\s+)?skate)\b', description_lower):
        property_data["pista_skate"] = True

    # ========================================================================
    # GROUP 9: LEISURE CALCULATION
    # ========================================================================

    leisure_fields = [
        "sala_ginastica", "salao_de_festas", "piscina", "sauna", "salao_de_jogos",
        "quadra_esportiva", "cinema", "playground", "churrasqueira"
    ]
    leisure_count = sum(1 for field in leisure_fields if property_data.get(field))
    property_data["lazer_completo"] = leisure_count >= 3
    property_data["lazer_parcial"] = 1 <= leisure_count < 3

    # ========================================================================
    # GROUP 10: IDs & FEATURES
    # ========================================================================

    def extract_html_features(soup: BeautifulSoup) -> list:
        """
        Extract features from the HTML feature list (not from aviso_info).
        """
        html_features = []
        
        # Look for the features container
        feature_container = soup.find('div', class_=re.compile(r'generalFeaturesProperty-module__description-container'))
        
        if feature_container:
            # Extract all feature text from span elements
            feature_spans = feature_container.find_all('span', class_=re.compile(r'generalFeaturesProperty-module__description-text'))
            
            for span in feature_spans:
                feature_text = span.get_text(strip=True)
                if feature_text:
                    html_features.append(feature_text)
        
        return html_features

    property_data["anunciante_id"] = str(aviso_info.get('postingCode', '')) if aviso_info else ""
    property_data["anuncio_id"] = str(aviso_info.get('idAviso', '')) if aviso_info else ""
    property_data["features"] = extract_html_features(soup)

    return property_data
