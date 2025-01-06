import asyncio
import time

import aiohttp
from database.db_operations import salvar_dados, get_dataframe

# Cabeçalhos HTTP para as requisições
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


# Função para fazer requisições HTTP e salvar no banco
async def make_request(url, session):
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
            salvar_dados([(content, url)], "imagens")
            return status
    except Exception as e:
        print(f"Erro na requisição : {e}")
        return None


# Função principal para processar requisições
async def main(total_requests, recovery_time=10):
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
            print(
                f"Iniciando requisição {completed_requests + 1} de {len(total_requests)}"
            )

            # Realiza a requisição
            status = await make_request(url, session)

            # Verifica se o status é 202
            while status == 202:
                print(
                    f"Status 202 recebido na requisição {completed_requests + 1}. Aguardando {recovery_time} segundos..."
                )
                await asyncio.sleep(recovery_time)  # Pausa assíncrona
                status = await make_request(url, session)  # Tenta novamente

            # Incrementa o contador somente após sair do loop 202
            completed_requests += 1

    elapsed_time = time.time() - start_time
    print(f"Todas as requisições concluídas. Tempo total: {elapsed_time:.2f} segundos")


def baixar_imagem():
    # Executar as requisições e salvar imagens no banco
    df = get_dataframe("SELECT link_imagem FROM imagens")["link_imagem"].tolist()
    asyncio.run(main(df))
