import asyncio

import aiohttp
from tqdm import tqdm

from database.db_operations import get_image_links, salvar_dados
from scraper.utils.request_async import fetch_async


async def baixar_imagem(linhas=20000):
    """Processa requisições assíncronas e salva imagens no banco de dados.

    :param linhas: Numero maximo de urls a serem baixadas
    """
    total_requests = get_image_links()[:linhas]
    async with aiohttp.ClientSession() as session:
        with tqdm(total=len(total_requests), desc="Progresso") as pbar:
            tasks = [fetch_async(session, url, pbar=pbar) for url in total_requests]
            results = await asyncio.gather(*tasks)

    salvar_dados([(content, url) for url, content in results], "imagens")
