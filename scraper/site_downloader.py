import asyncio
import logging
import time

import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm

from database import (
    close_gap,
    execute_today,
    get_null_product_category,
    processar_dados_brutos,
    salvar_disponibilidade,
    salvar_preco,
    salvar_produto,
    set_cidades,
)
from scraper.cookies.load_cookies import load_cookie
from scraper.network.request_async import fetch_async
from scraper.utils.categories import get_categories

logger = logging.getLogger(__name__)

def extrair_dados(soup):
    # Nomes e links dos produtos
    nome_link = [
        i.find("a")
        for i in soup.find_all(class_="h-[72px] text-ellipsis overflow-hidden cursor-pointer mt-2 text-center")
    ]
    nome = [nome.text.strip() for nome in nome_link if nome]
    link = ["https://www.irmaosgoncalves.com.br" + link.get("href") for link in nome_link if link and link.get("href")]

    # Preços dos produtos
    preco = [a.text.strip() for a in soup.find_all("div", class_="text-xl text-secondary font-semibold h-7")]

    return nome, preco, link


def verificar_tamanhos(nome, preco, link):
    if len(nome) != len(preco) != len(link):
        msg = f"Nome={len(nome)}, Preço={len(preco)}, Link={len(link)}"
        raise ValueError(msg)


async def process_url(session, url, cookies, categoria, cidade, pbar):
    content = await fetch_async(session, url, cookies, pbar)
    if not content:
        return [], [], cidade

    #logger.info("%s - %s", cidade, url.split("?")[0].split("/")[-1])
    soup = BeautifulSoup(content, "html.parser")
    nome_prod, preco, link = extrair_dados(soup)

    verificar_tamanhos(nome_prod, preco, link)

    produtos = [(n, l, categoria) for n, l in zip(nome_prod, link)]
    precos = [(l, float(p.replace("R$", "").replace(".", "").replace(",", ".").strip())) for p, l in zip(preco, link)]

    return produtos, precos, cidade


async def baixar_site():
    # se ja execultou hoje, nao execultar novamente
    if execute_today():
        logger.info(f"Ja executou hoje dia: {execute_today().data_atualizacao}")
        return

    inicio1 = time.time()
    cookies = load_cookie("requests")
    set_cidades([cidade for cidade, _ in cookies])

    url_base = "https://www.irmaosgoncalves.com.br"
    urls_folha, urls_raiz, categorias = get_categories(url_base)
    urls = urls_folha

    # se tiver menos de 100 produtos sem categoria baixar os produtos sem a categoria para ir mais rapido
    logger.info(f"Produtos sem categoria: {len(get_null_product_category())}")
    if len(get_null_product_category()) < 10000:
        urls = urls_raiz
        categorias = len(urls) * [None]

    # fazer as requests de forma assíncrona
    async with aiohttp.ClientSession() as session:
        with tqdm(total=len(urls)*len(cookies), desc="Progresso") as pbar:
            tasks = [
                process_url(session, url, cookie, categoria, cidade, pbar)
                for url, categoria in zip(urls, categorias)
                for cidade, cookie in cookies
            ]
            resultados_brutos = await asyncio.gather(*tasks)

    close_gap()
    dados_processados = processar_dados_brutos(resultados_brutos)

    salvar_produto(dados_processados.produtos)

    salvar_preco(dados_processados.precos_uniformes, dados_processados.precos_variaveis)

    salvar_disponibilidade(dados_processados.disponibilidades)

    logger.info(f"Produtos disponives: {len(dados_processados.disponibilidades)}")


    fim1 = time.time()
    logger.info(f"Tempo de execução dos total: {(fim1 - inicio1) / 60:.2f} minutos.")
