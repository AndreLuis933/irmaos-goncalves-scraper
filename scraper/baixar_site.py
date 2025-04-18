import asyncio
import logging
import time

import aiohttp
from bs4 import BeautifulSoup

from database.db_operations import (
    execute_today,
    get_null_product_category,
    price_change,
    save_price,
    save_product,
    set_cidades,
)
from scraper.utils.categories import get_categories
from scraper.utils.load_cookies import load_cookie
from scraper.utils.request_async import fetch_async


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


async def process_url(session, url, cookies, categoria, cidade):
    content = await fetch_async(session, url, cookies)
    if not content:
        return [], []

    logging.info("%s - %s", cidade, url.split("?")[0].split("/")[-1])
    soup = BeautifulSoup(content, "html.parser")
    nome_prod, preco, link = extrair_dados(soup)

    verificar_tamanhos(nome_prod, preco, link)

    produtos = [(n, l, categoria) for n, l in zip(nome_prod, link)]
    precos = [(l, float(p.replace("R$", "").replace(".", "").replace(",", ".").strip())) for p, l in zip(preco, link)]

    return produtos, precos, cidade


async def baixar_site():
    # se ja execultou hoje, nao execultar novamente
    if execute_today():
        logging.info(f"Ja executou hoje dia: {execute_today().data_atualizacao}")
        # return

    inicio1 = time.time()
    cookies = load_cookie("requests")
    set_cidades([cidade for cidade, _ in cookies])
    return

    url_base = "https://www.irmaosgoncalves.com.br"
    urls_folha, urls_raiz, categorias = get_categories(url_base)
    urls = urls_folha

    # se tiver menos de 100 produtos sem categoria baixar os produtos sem a categoria para ir mais rapido
    logging.info(f"Produtos sem categoria: {get_null_product_category()}")
    if get_null_product_category() < 10000000:
        urls = urls_raiz
        categorias = len(urls) * [None]

    # fazer as requests de forma assíncrona
    async with aiohttp.ClientSession() as session:
        tasks = [
            process_url(session, url, cookie, categoria, cidade)
            for url, categoria in zip(urls, categorias)
            for cidade, cookie in cookies
        ]
        resultados = await asyncio.gather(*tasks)

    import pickle

    with open("dados_produtos.pickle", "wb") as arquivo:
        pickle.dump(resultados, arquivo)
    # [([[nome,link,categoria],[nome,link,categoria]], [[link,preco],[link,preco]],cidade), ([[nome,link,categoria],[nome,link,categoria]], [[link,preco],[link,preco]],cidade)]
    # separa as listas que estavam assim [(produtos, precos,cidade), (produtos, precos,cidade)] para uma com todos os produtos e outra com todos os preços  # noqa: E501
    produtos_para_salvar = [produto for produtos, _ in resultados for produto in produtos]
    precos_para_salvar = [preco for _, precos in resultados for preco in precos]

    inicio = time.time()
    save_product(produtos_para_salvar)
    fim = time.time()
    logging.info(f"Tempo de execução dos produtos: {fim - inicio:.2f} segundos.")

    inicio = time.time()
    save_price(precos_para_salvar)
    fim = time.time()
    logging.info(f"Tempo de execução dos preços: {fim - inicio:.2f} segundos.")

    fim1 = time.time()
    logging.info(f"Tempo de execução dos total: {fim1 - inicio1:.2f} segundos.")
    print(len(produtos_para_salvar))

    # ver quantos produtos mudaram de preço desde a primeira execução
    price_change()
