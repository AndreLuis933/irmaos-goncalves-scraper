import asyncio
import logging

import aiohttp

from scraper.config.request_config import HEADERS
from scraper.utils.selenium_helpers import calculate_delay

logger = logging.getLogger(__name__)


async def fetch_async(session, url, cookies=None, pbar=None, tipo="produtos", max_retries=25):
    if cookies is None:
        cookies = {}
    delay = calculate_delay(1)
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=HEADERS, cookies=cookies) as response:
                if response.status == 200:
                    pbar.update(1)
                    if tipo == "imagens":
                        content = await response.read()
                        return url, content
                    return await response.text()
                if response.status == 503 and tipo == "imagens":
                    pbar.update(1)
                    logger.warning(f"Status {response.status} nao fazer mais requesicoes para {url}")
                    return (None, None)
                if attempt < max_retries:
                    delay = calculate_delay(attempt)
                    # logger.warning(f"Status {response.status} recebido. Aguardando {delay:.2f} segundos.")

                    await asyncio.sleep(delay)
        except aiohttp.ClientError:  # noqa: PERF203
            logger.exception(f"Erro ao fazer requisição para {url}")
            await asyncio.sleep(delay)

    logger.error(f"Falha após {max_retries} tentativas para {url}")
    pbar.update(1)
    if pbar:
        return (None, None)
    return None
