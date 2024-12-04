import aiohttp
import asyncio
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from sqlalchemy.sql import text
import re
import os

# Cabeçalhos HTTP para as requisições
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
MODIFICADORES = ["_1", "_99_1", "_1_1", "_1_3", "_99_3"]
EXTENSOES = [".jpg", ".png"]

# Determinar o diretório do arquivo em execução
base_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(base_dir, "produtos.db")

# Criar engine com o caminho absoluto
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
    match = re.search(r'-(\d+)$', link)
    if match:
        return match.group(1)
    return None


def salvar_imagem_no_banco(lista_imagens, session):
    """
    Atualiza o campo 'conteudo' na tabela 'imagens' com os dados binários da imagem.
    :param url: URL da imagem (string)
    :param imagem_binaria: Conteúdo da imagem em bytes
    :param session: Sessão do SQLAlchemy
    """
    try:
        # Query de atualização usando text
        query = text("INSERT INTO imagens (link_imagem, produto_id) VALUES (:url, :id)")
        session.execute(query, [{"url": url, "id": id} for id, url in lista_imagens])
        session.commit()
        print(f"Batch de {len(lista_imagens)} imagens salvo com sucesso no banco.")
    except Exception as e:
        session.rollback()
        print(f"Erro ao salvar o batch no banco: {e}")


# Função para fazer requisições HTTP e salvar no banco
async def make_request(url, session, batch, id):
    """
    Envia uma requisição HTTP assíncrona e salva a imagem no banco de dados.
    :param url: URL para fazer a requisição
    :param session: Sessão HTTP assíncrona
    :param db_session: Sessão do banco de dados
    """
    try:
        async with session.head(url, headers=HEADERS) as response:
            status = response.status
            if status == 200:
                batch.append((id, url))

            return status
    except Exception as e:
        print(f"Erro na requisição : {e}")
        return None


# Função principal para processar requisições
async def main(total_requests, db_session, recovery_time=10, batch_size=50):
    """
    Processa requisições assíncronas e salva imagens no banco de dados.
    :param total_requests: Lista de URLs para processar
    :param db_session: Sessão do banco de dados
    :param recovery_time: Tempo de espera em caso de status 202
    """
    start_time = time.time()
    completed_requests = 0  # Contador de requisições concluídas
    total = 0

    async with aiohttp.ClientSession() as session:
        for row in total_requests.itertuples(index=False):
            completed_requests += 1
            id, link = row
            codigo = extrair_id(link)
            encontrou_valido = False
            batch = []

            base_url = f"https://conteudo.irmaosgoncalves.com.br/produto/{codigo}/{codigo}"
            for modificador in MODIFICADORES:
                if encontrou_valido:
                    break
                for extensao in EXTENSOES:
                    url = f"{base_url}{modificador}{extensao}?width=400"
                    print(f"Iniciando requisição {completed_requests} de {len(total_requests)}")
                    status = await make_request(url, session, batch, id)

                    while status == 202:
                        print(
                            f"Status 202 recebido na requisição {completed_requests}. Aguardando {recovery_time} segundos...")
                        await asyncio.sleep(recovery_time)
                        status = await make_request(url, session, batch, id)
                        
                    if status == 404:
                        continue

                    encontrou_valido = True
                    total += 1
                    break

            if len(batch) >= batch_size:
                salvar_imagem_no_banco(batch, db_session)
                batch.clear()  # Limpar o lote após o salvamento

        # Salvar o restante no final, se houver
        if batch:
            salvar_imagem_no_banco(batch, db_session)

    elapsed_time = time.time() - start_time
    print(f"Todas as requisições concluídas. Tempo total: {elapsed_time:.2f} segundos")
    print(total)


def baixar_imagem():
    df = pd.read_sql_query(query, engine)
    df = df.head(200)
    # Executar as requisições e salvar imagens no banco
    asyncio.run(main(total_requests=df, db_session=session))


baixar_imagem()

# sem nenhuma pausa 29.09 s
# 183 de 200
# 6,89 por segundo
