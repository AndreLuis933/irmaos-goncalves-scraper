import logging
import time
from collections import defaultdict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scraper.config.requests import HEADERS


def fetch(url, cookies=None, max_retries=6, initial_delay=10):
    try:
        for attempt in range(1, max_retries):
            with requests.get(url, headers=HEADERS, cookies=cookies) as response:
                if response.status_code == 200:
                    return response.content

                delay = initial_delay * (2**attempt)
                logging.warning(f"Status {response.status_code} recebido. Aguardando {delay} segundos.")

                time.sleep(delay)
    except requests.RequestException:
        logging.exception(f"Erro ao fazer requisição para {url}")
        time.sleep(5)

    logging.error(f"Falha após {max_retries} tentativas para {url}")
    return None


def get_categories(url_base):
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
