import asyncio
import re
import time
from collections import Counter

import aiohttp

from database.db_operations import get_dataframe, save_images
from scraper.config.requests import HEADERS

#modulo nao nessesario mais


MODIFICADORES = ["_1", "_99_1", "_1_1", "_1_3", "_99_3"]
EXTENSOES = [".jpg", ".png"]


def extrair_id(link):
    """Extrai o ID único do link."""
    match = re.search(r"-(\d+)$", link)
    if match:
        return match.group(1)
    return None


async def testar_link(total_links, recovery_time=5, pacote_size=50):
    """Tenta prever a imagem correspondente ao link.

    Args:
        total_links (pandas df): df com os links
        recovery_time (int, optional): tempo de espera em caso de bloqueio. Defaults to 5.
        pacote_size (int, optional): tamanho do pacote para ser salvo. Defaults to 50.

    """
    inicio = time.time()
    requisisoes_feitas = 0
    bem_susedidas = 0
    pacote = []
    qnt_requests = []

    async with aiohttp.ClientSession() as nav:
        for row in total_links.itertuples(index=False):
            requisisoes_feitas += 1
            id_row, link = row
            codigo = extrair_id(link)
            encontrou_valido = False
            url_base = f"https://conteudo.irmaosgoncalves.com.br/produto/{codigo}/{codigo}"
            for modificador in MODIFICADORES:
                if encontrou_valido:
                    break

                for extensao in EXTENSOES:
                    url = f"{url_base}{modificador}{extensao}?width=400"
                    print(f"Iniciando requisição {requisisoes_feitas} de {len(total_links)}")
                    qnt_requests.append((id_row, link))

                    async with nav.head(url, headers=HEADERS) as response:
                        status = response.status

                    while status == 202:
                        print(f"Status 202 recebido na requisição {requisisoes_feitas}. Aguardando {recovery_time}")
                        await asyncio.sleep(recovery_time)

                        async with nav.head(url, headers=HEADERS) as response:
                            status = response.status
                    if status == 404:
                        continue

                    if status == 200:
                        pacote.append((id_row, url))
                        encontrou_valido = True
                        bem_susedidas += 1
                        break

                    print(status)

            if len(pacote) >= pacote_size:
                save_images(pacote)
                pacote.clear()

        if pacote:
            save_images(pacote)

    tempo_gasto = time.time() - inicio
    print(f"Todas as requisições concluídas. Tempo total: {tempo_gasto:.2f} segundos")
    print(f"Total de requisiçoes sucedidas: {bem_susedidas}")
    print(f"Media de tempo por requesiçao:{len(total_links) / tempo_gasto:.2f}")
    counts = Counter(qnt_requests)
    filtered_counts = {key: value for key, value in counts.items() if value == 10}

    print(filtered_counts)


def prever_imagem():
    df = get_dataframe("""SELECT id, link FROM produtos WHERE id NOT IN (SELECT produto_id FROM imagens);""")
    df = df.head(200)
    asyncio.run(testar_link(total_links=df))


# sem nenhuma pausa 29.09 s
# 183 de 200
# 6,89 por segundo
