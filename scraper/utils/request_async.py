import asyncio
import logging
import random

import aiohttp

from scraper.config.requests import HEADERS

max_concurrent_requests = 1500
semaphore = asyncio.Semaphore(max_concurrent_requests)


def calculate_delay(attempt, base_delay=60, increment=30, max_delay=600, jitter_factor=0.1):
    delay = min(base_delay + (attempt * increment), max_delay)
    jitter = random.uniform(0, delay * jitter_factor)
    return delay + jitter



async def fetch_async(session, url, cookies=None, pbar=None, max_retries=5):
    if cookies is None:
        cookies = {}

    try:
        for attempt in range(1, max_retries+1):
            async with session.get(url, headers=HEADERS, cookies=cookies) as response:
                if response.status == 200:
                    if pbar:
                        pbar.update(1)
                        content = await response.read()
                        return url, content
                    return await response.text()
                if response.status == 503:
                    pbar.update(1)
                    logging.warning(f"Status {response.status} nao fazer mais requesicoes para {url}")
                    return (None, None)
                if attempt < max_retries:
                    delay = calculate_delay(attempt)
                    logging.warning(f"Status {response.status} recebido. Aguardando {delay:.2f} segundos.")

                    await asyncio.sleep(delay)
    except aiohttp.ClientError:
        logging.exception(f"Erro ao fazer requisição para {url}")
        await asyncio.sleep(5)

    logging.error(f"Falha após {max_retries - 1} tentativas para {url}")
    if pbar:
        pbar.update(1)
        return (None, None)
    return None
