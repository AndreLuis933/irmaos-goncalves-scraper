import asyncio
import time

import aiohttp
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# Cabeçalhos HTTP para as requisições
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}


engine = create_engine("sqlite:///produtos.db")
Session = sessionmaker(bind=engine)
session = Session()
query = "SELECT link_imagem FROM imagens"


def salvar_imagem_no_banco(url, imagem_binaria, session):
    """
    Atualiza o campo 'conteudo' na tabela 'imagens' com os dados binários da imagem.
    :param url: URL da imagem (string)
    :param imagem_binaria: Conteúdo da imagem em bytes
    :param session: Sessão do SQLAlchemy
    """
    try:
        # Query de atualização usando text
        query = text("UPDATE imagens SET conteudo = :conteudo WHERE link_imagem = :url")
        session.execute(query, {"conteudo": imagem_binaria, "url": url})
        session.commit()
        print(f"Imagem salva no banco com sucesso para o URL: {url}")
    except Exception as e:
        session.rollback()
        print(f"Erro ao salvar a imagem no banco para o URL {url}: {e}")

# Função para fazer requisições HTTP e salvar no banco
async def make_request(url, session, db_session):
    """
    Envia uma requisição HTTP assíncrona e salva a imagem no banco de dados.
    :param url: URL para fazer a requisição
    :param session: Sessão HTTP assíncrona
    :param db_session: Sessão do banco de dados
    """
    try:
        async with session.get(url, headers=HEADERS) as response:
            status = response.status
            if status == 202:
                return status

            # Lê o conteúdo da resposta como bytes
            content = await response.read()

            # Salvar a imagem no banco
            salvar_imagem_no_banco(url, content, db_session)
            return status
    except Exception as e:
        print(f"Erro na requisição : {e}")
        return None

# Função principal para processar requisições
async def main(total_requests, db_session, recovery_time=10):
    """
    Processa requisições assíncronas e salva imagens no banco de dados.
    :param total_requests: Lista de URLs para processar
    :param db_session: Sessão do banco de dados
    :param recovery_time: Tempo de espera em caso de status 202
    """
    start_time = time.time()
    completed_requests = 0  # Contador de requisições concluídas

    async with aiohttp.ClientSession() as session:
        for url in total_requests:
            print(f"Iniciando requisição {completed_requests + 1} de {len(total_requests)}")
            
            # Realiza a requisição
            status = await make_request(url, session, db_session)

            # Verifica se o status é 202
            while status == 202:
                print(f"Status 202 recebido na requisição {completed_requests + 1}. Aguardando {recovery_time} segundos...")
                await asyncio.sleep(recovery_time)  # Pausa assíncrona
                status = await make_request(url, session, db_session)  # Tenta novamente

            # Incrementa o contador somente após sair do loop 202
            completed_requests += 1

    elapsed_time = time.time() - start_time
    print(f"Todas as requisições concluídas. Tempo total: {elapsed_time:.2f} segundos")
    

def baixar_imagem():
    df = pd.read_sql_query(query, engine)['link_imagem'].to_list()
    # Executar as requisições e salvar imagens no banco
    asyncio.run(main(total_requests=df, db_session=session))

