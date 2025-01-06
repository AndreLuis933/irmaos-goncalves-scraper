from datetime import datetime, timezone
import os
import pandas as pd
from .db_setup import Session, Produto, HistoricoPreco, Imagem, ENGINE, Base
import logging

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def database_exists():
    return os.path.exists(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "produtos.db"
        )
    )


def create_tables():
    Base.metadata.create_all(ENGINE)


if not database_exists():
    create_tables()


def get_dataframe(query):
    """
    Retorna um DataFrame do Pandas com os resultados da consulta SQL.
    """
    return pd.read_sql_query(query, ENGINE)


def produtos(dados, session):
    objetos = []
    for nome, link in dados:
        produto = session.query(Produto).filter_by(link=link).first()
        if produto:
            produto.nome = nome
            produto.link = link
        else:
            produto = Produto(nome=nome, link=link)
            objetos.append(produto)

    return objetos


def historico_preco(dados, session):
    objetos = []
    for link, preco in dados:
        produto = session.query(Produto).filter_by(link=link).first()

        hoje = datetime.now(timezone.utc).date()
        registro_existente = (
            session.query(HistoricoPreco)
            .filter_by(produto_id=produto.id, data_atualizacao=hoje)
            .first()
        )

        if not registro_existente:
            objetos.append(
                HistoricoPreco(
                    produto_id=produto.id, preco=preco, data_atualizacao=hoje
                )
            )

    return objetos


def imagens(dados, session):
    """
    Processa dados de imagens, atualizando ou criando registros no banco de dados.

    Parâmetros:
    - dados (list of tuple): Lista de tuplas com (link, conteudo), onde `conteudo` pode ser bytes para atualizar imagens existentes ou int para criar novas com `produto_id`.
    - session (Session): Sessão de banco de dados SQLAlchemy para operações de consulta e atualização.

    Retorna:
    - list: Lista de novos objetos `Imagem` para persistência.
    """

    objetos = []
    for conteudo, link in dados:
        if isinstance(conteudo, bytes):
            imagem = session.query(Imagem).filter_by(link_imagem=link).first()
            imagem.conteudo = conteudo
        elif isinstance(conteudo, int):
            objetos.append(Imagem(produto_id=conteudo, link_imagem=link))
    return objetos


def salvar_dados(dados, tipo):
    """
    Salva dados em massa no banco de dados.

    Parâmetros:
    - dados (list): Lista de dados a serem salvos.
    - tipo (str): Tipo de dados a serem processados ('produtos', 'historico_preco', 'imagens').

    Retorna:
    - None
    """
    processadores = {
        "produtos": produtos,
        "historico_preco": historico_preco,
        "imagens": imagens,
    }

    # Obtenha a função de processamento correta
    funcao_processamento = processadores.get(tipo)

    if not funcao_processamento:
        logger.error(f"Tipo de processamento '{tipo}' não é válido.")
        return

    session = Session()
    try:
        objetos = funcao_processamento(dados, session)
        session.bulk_save_objects(objetos)
        session.commit()
        logger.info(
            f"{len(objetos)} registros de {tipo} salvos ou atualizados com sucesso."
        )
    except Exception as e:
        session.rollback()
        logger.error(f"Erro ao salvar registros de {tipo}: {e}")
    finally:
        session.close()
