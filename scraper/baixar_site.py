import asyncio
import json
import logging
import time
from collections import defaultdict
from itertools import chain
from urllib.parse import urljoin

import aiohttp
import requests
from bs4 import BeautifulSoup

from database.db_operations import (
    execute_today,
    get_count_products_without_images,
    get_image_links,
    get_null_product_category,
    price_change,
    salvar_dados,
)

from .images.get_links import get_images
from .utils.request_async import fetch_async

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    ),
}


def carregar_cookies():
    try:
        with open("cookies.json") as file:
            cookies = json.load(file)
            cookies["app"] = cookies["app"]["value"]
            return cookies
    except Exception:
        logging.exception("Erro ao carregar cookies: ")
        raise


def fetch(url, cookies=None, max_retries=6, initial_delay=10):
    try:
        for attempt in range(1, max_retries):
            with requests.get(url, headers=HEADERS, cookies=cookies) as response:
                if response.status_code == 200:
                    return response.content
                if response.status_code in (202, 429):
                    delay = initial_delay * (2**attempt)
                    logging.warning(f"Status {response.status_code} recebido. Aguardando {delay} segundos.")

                time.sleep(delay)
    except aiohttp.ClientError:
        logging.exception(f"Erro ao fazer requisição para {url}")
        time.sleep(5)

    logging.error(f"Falha após {max_retries} tentativas para {url}")
    return None


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


def obter_categorias_e_processar(url_base):
    response = fetch(url_base)

    soup = BeautifulSoup(response, "html.parser")
    links = soup.find("ul").find_all("a") if soup.find("ul") else []

    categorias = defaultdict(list)
    categorias_raiz = set()
    urls_folha = []
    nomes_arquivos = []

    for link in links:
        url = link.get("href")
        if not url:
            continue

        partes = url.split("/")[2:]
        raiz = partes[0]
        categorias_raiz.add(raiz)

        for i in range(len(partes)):
            chave = "/".join(partes[: i + 1])
            categorias[chave].append(url)

    for chave, subcategorias in categorias.items():
        if len(subcategorias) == 1:
            url = subcategorias[0]
            urls_folha.append(urljoin(url_base, url + "?p=10000"))
            nomes_arquivos.append(chave)

    urls_base = [urljoin(url_base, f"/categoria/{raiz}?p=10000") for raiz in categorias_raiz]

    return urls_folha, urls_base, nomes_arquivos


async def process_url(session, url, cookies, nome):
    content = await fetch_async(session, url, cookies)
    if not content:
        return [], []

    logging.info(url.split("?")[0].split("/")[-1])
    soup = BeautifulSoup(content, "html.parser")
    nome_prod, preco, link = extrair_dados(soup)

    verificar_tamanhos(nome_prod, preco, link)

    produtos = [(n, l, nome) for n, l in zip(nome_prod, link)]
    precos = [(l, float(p.replace("R$", "").replace(".", "").replace(",", ".").strip())) for p, l in zip(preco, link)]

    return produtos, precos


async def baixar_site():
    # se ja execultou hoje, nao execultar novamente
    if execute_today():
        print(execute_today().data_atualizacao)
        #return

    cookies = carregar_cookies()
    url_base = "https://www.irmaosgoncalves.com.br"
    urls_folha, urls_raiz, nomes_arquivos = obter_categorias_e_processar(url_base)
    urls = urls_folha

    # se tiver menos de 100 produtos sem categoria baixar os produtos sem a categoria para ir mais rapido
    logging.info(f"Produtos sem categoria: {get_null_product_category()}")
    if get_null_product_category() < 100:
        urls = urls_raiz
        nomes_arquivos = len(urls) * [None]

    # fazer as requests de forma assíncrona
    async with aiohttp.ClientSession() as session:
        tasks = [process_url(session, url, cookies, nome) for url, nome in zip(urls, nomes_arquivos)]
        resultados = await asyncio.gather(*tasks)

    # separa as listas que estavam assim [(produtos, precos), (produtos, precos)] para uma com todos os produtos e outra com todos os preços  # noqa: E501
    produtos_para_salvar = [produto for produtos, _ in resultados for produto in produtos]
    precos_para_salvar = [preco for _, precos in resultados for preco in precos]

    salvar_dados(produtos_para_salvar, "produtos")

    salvar_dados(precos_para_salvar, "historico_preco")

    # se tiver mais de 5000 sem imagens baixar as imagens
    logging.info(f"Produtos sem imagens: {get_count_products_without_images()}")
    if get_count_products_without_images() > 20:
        get_images(urls_raiz)

    # ver quantos produtos mudaram de preço desde a primeira execução
    price_change()
