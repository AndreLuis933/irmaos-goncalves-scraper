import asyncio
from collections import Counter
import os
import re
import time

import aiohttp
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# Cabeçalhos HTTP para as requisições
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/'
    '537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    
}
MODIFICADORES = ["_1", "_99_1", "_1_1", "_1_3", "_99_3"]
EXTENSOES = [".jpg", ".png"]

base_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(base_dir, "produtos.db")

engine = create_engine(f"sqlite:///{db_path}")
Session = sessionmaker(bind=engine)
session = Session()
query = """
SELECT id, link
FROM produtos
WHERE id NOT IN (SELECT produto_id FROM imagens);
"""


def extrair_id(link):
    """Extrai o ID único do link."""
    match = re.search(r"-(\d+)$", link)
    if match:
        return match.group(1)
    return None


def salvar_imagem_no_banco(lista_imagens):
    """salva as imagens no banco de dados

    Args:
        lista_imagens (list): lista com links de imagems para serem salvas
    """
    try:
        query = text("INSERT INTO imagens (link_imagem, produto_id) VALUES (:url, :id)")
        session.execute(query, [{"url": url, "id": id} for id, url in lista_imagens])
        session.commit()
        print(f"Pacote de {len(lista_imagens)} imagens salvo com sucesso no banco.")
    except Exception as e:
        session.rollback()
        print(f"Erro ao salvar o pacote no banco: {e}")


async def testar_link(total_links, recovery_time=5, pacote_size=50):
    """tenta prever a imagem correspondente ao link

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
            id, link = row
            codigo = extrair_id(link)
            encontrou_valido = False
            url_base = (
                f"https://conteudo.irmaosgoncalves.com.br/produto/{codigo}/{codigo}"
            )
            for modificador in MODIFICADORES:
                if encontrou_valido:
                    break

                for extensao in EXTENSOES:
                    url = f"{url_base}{modificador}{extensao}?width=400"
                    print(
                        f"Iniciando requisição {requisisoes_feitas} de {len(total_links)}"
                    )
                    qnt_requests.append((id,link))

                    async with nav.head(url, headers=HEADERS) as response:
                        status = response.status

                    while status == 202:
                        print(
                            f"Status 202 recebido na requisição {requisisoes_feitas}. Aguardando {recovery_time} segundos..."
                        )
                        await asyncio.sleep(recovery_time)

                        async with nav.head(url, headers=HEADERS) as response:
                            status = response.status
                    if status == 404:
                        continue

                    if status == 200:
                        pacote.append((id, url))
                        encontrou_valido = True
                        bem_susedidas += 1
                        break

                    print(status)

            if len(pacote) >= pacote_size:
                salvar_imagem_no_banco(pacote)
                pacote.clear()

        if pacote:
            salvar_imagem_no_banco(pacote)

    tempo_gasto = time.time() - inicio
    print(f"Todas as requisições concluídas. Tempo total: {tempo_gasto:.2f} segundos")
    print(f"Total de requisiçoes sucedidas: {bem_susedidas}")
    print(f"Media de tempo por requesiçao:{len(total_links)/tempo_gasto:.2f}")
    counts = Counter(qnt_requests)
    filtered_counts = {key: value for key, value in counts.items() if value == 10}

    print(filtered_counts)
    


def prever_imagem():
    df = pd.read_sql_query(query, engine)
    df = df.head(20)
    asyncio.run(testar_link(total_links=df))


# sem nenhuma pausa 29.09 s
# 183 de 200
# 6,89 por segundo