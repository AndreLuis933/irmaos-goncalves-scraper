import json
import logging

import requests
from bs4 import BeautifulSoup
from database.db_operations import salvar_dados

# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configurações globais
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def carregar_cookies():
    """
    Carrega os cookies de um arquivo JSON.
    """
    try:

        with open("cookies.json", "r") as file:
            cookies = json.load(file)
            cookies["app"] = cookies["app"]["value"]
            return cookies
    except Exception as e:
        logging.error(f"Erro ao carregar cookies: {e}")
        raise


def obter_resposta(url, headers, cookies):
    """
    Realiza a requisição HTTP para a URL fornecida.
    """
    try:
        response = requests.get(url, cookies=cookies, headers=headers)
        if response.status_code == 200:
            return response
        else:
            logging.error(
                f"Falha na requisição para {url}: Código {response.status_code}"
            )
            return None
    except requests.RequestException as e:
        logging.error(f"Erro ao fazer requisição para {url}: {e}")
        return None


def extrair_dados(soup):
    """
    Extrai informações de nome, link, preço e imagem de um BeautifulSoup.
    """
    try:
        # Nomes e links dos produtos
        nome_link = [
            i.find("a")
            for i in soup.find_all(
                class_="h-[72px] text-ellipsis overflow-hidden cursor-pointer mt-2"
            )
        ]
        nome = [nome.text.strip() for nome in nome_link if nome]
        link = [
            "https://www.irmaosgoncalves.com.br" + link.get("href")
            for link in nome_link
            if link and link.get("href")
        ]

        # Preços dos produtos
        preco = [
            a.text.strip()
            for a in soup.find_all(
                "div", class_="text-xl text-secondary font-semibold h-7"
            )
        ]

        return nome, preco, link
    except Exception as e:
        logging.error(f"Erro ao extrair dados: {e}")
        return [], [], []


def verificar_tamanhos(nome, preco, link):
    """
    Verifica se todas as listas têm o mesmo tamanho.
    """
    list_sizes = [len(nome), len(preco), len(link)]
    if len(set(list_sizes)) != 1:
        logging.error(
            f"As listas têm tamanhos diferentes! Tamanhos: Nome={len(nome)}, Preço={len(preco)}, Link={len(link)}"
        )
        raise ValueError("As listas extraídas não possuem o mesmo tamanho.")


def obter_categorias_e_processar(url_base):
    """
    Obtém as categorias do site, gera as URLs e processa os dados de cada categoria.
    """
    try:
        # Requisição à página base para obter as categorias
        response = requests.get(url_base, headers=HEADERS)
        soup = BeautifulSoup(response.content, "html.parser")

        # Encontre os itens da lista suspensa
        lista = [i.get("href") for i in soup.find("ul").find_all("a")]

        categorias_filtradas = set()
        urls = []
        nomes_arquivos = []

        for categoria in lista:
            raiz = categoria.split("/categoria/")[1].split("/")[0]
            if raiz not in categorias_filtradas:
                categorias_filtradas.add(raiz)
                urls.append(url_base + categoria + "?p=10000")
                nomes_arquivos.append(raiz)

        # Processar as URLs geradas
        return urls, nomes_arquivos
    except Exception as e:
        logging.error(f"Erro ao obter categorias e processar: {e}")


def baixar_site():
    url_base = "https://www.irmaosgoncalves.com.br"
    urls, nomes_arquivos = obter_categorias_e_processar(url_base)

    cookies = carregar_cookies()
    produtos_para_salvar = []
    precos_para_salvar = []

    for url, nome in zip(urls, nomes_arquivos):
        try:
            logging.info(nome)
            response = obter_resposta(url, HEADERS, cookies)
            if response:
                soup = BeautifulSoup(response.content, "html.parser")
                nome_prod, preco, link = extrair_dados(soup)
                verificar_tamanhos(nome_prod, preco, link)

                for n, p, l in zip(nome_prod, preco, link):
                    produtos_para_salvar.append((n, l))
                    precos_para_salvar.append(
                        (
                            l,
                            float(
                                p.replace("R$", "")
                                .replace(".", "")
                                .replace(",", ".")
                                .strip()
                            ),
                        )
                    )

        except Exception as e:
            logging.error(f"Erro ao processar {url}: {e}")

    # print(len(produtos_para_salvar))
    salvar_dados(produtos_para_salvar, "produtos")

    salvar_dados(precos_para_salvar, "historico_preco")
