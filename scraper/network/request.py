import logging
import time

import requests

from scraper.config.request_config import HEADERS

logger = logging.getLogger(__name__)
def fetch(url, cookies=None, max_retries=6, initial_delay=10):
    try:
        for attempt in range(1, max_retries):
            with requests.get(url, headers=HEADERS, cookies=cookies) as response:
                if response.status_code == 200:
                    return response.content

                delay = initial_delay * (2**attempt)
                logger.warning(f"Status {response.status_code} recebido. Aguardando {delay} segundos.")

                time.sleep(delay)
    except requests.RequestException:
        logger.exception(f"Erro ao fazer requisição para {url}")
        time.sleep(5)

    logger.error(f"Falha após {max_retries} tentativas para {url}")
    return None
