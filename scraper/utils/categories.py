import logging
from collections import defaultdict
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scraper.network.request import fetch

logger = logging.getLogger(__name__)


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
            urls_folha.append(urljoin(url_base, url + "?p=300"))
            nomes_arquivos.append(chave)

    urls_base = [urljoin(url_base, f"/categoria/{raiz}?p=300") for raiz in categorias_raiz]

    return urls_folha, urls_base, nomes_arquivos
