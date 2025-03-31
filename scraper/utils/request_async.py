import asyncio
import logging

import aiohttp

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    ),
}

max_concurrent_requests = 1500
semaphore = asyncio.Semaphore(max_concurrent_requests)


async def fetch_async(session, url, cookies=None, pbar=None, max_retries=5, initial_delay=10):
    if cookies is None:
        cookies = {}

    try:
        for attempt in range(1, max_retries):
            async with session.get(url, headers=HEADERS, cookies=cookies) as response:
                if response.status == 200:
                    if pbar:
                        pbar.update(1)
                        content = await response.read()
                        return url, content
                    return await response.text()

                delay = initial_delay * (2**attempt)
                logging.warning(f"Status {response.status} recebido. Aguardando {delay} segundos.")

                await asyncio.sleep(delay)
    except aiohttp.ClientError:
        logging.exception(f"Erro ao fazer requisição para {url}")
        await asyncio.sleep(5)

    logging.error(f"Falha após {max_retries - 1} tentativas para {url}")
    if pbar:
        pbar.update(1)
        return (None, None)
    return None
