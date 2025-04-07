from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import and_, func, not_

from .db_setup import ENGINE, HistoricoPreco, Imagem, Produto, Session


def get_dataframe(query):
    """Retorna um DataFrame do Pandas com os resultados da consulta SQL."""
    return pd.read_sql_query(query, ENGINE)

def get_image_links():
    with Session() as session:
        imagens = (
            session.query(Imagem)
            .filter(Imagem.conteudo.is_(None))
            .filter(not_(Imagem.link_imagem.like("%removebg-preview%")))
            .all()
        )
        return [imagem.link_imagem for imagem in imagens]

def produtos(dados, session):
    objetos = []
    for nome, link, categoria in dados:
        produto = session.query(Produto).filter_by(link=link).first()
        if produto:
            produto.nome = nome
            produto.link = link
            produto.categoria = categoria if categoria else produto.categoria
        else:
            produto = Produto(nome=nome, link=link, categoria=categoria)
            objetos.append(produto)

    return objetos


def historico_preco(dados, session):
    objetos = []
    for link, preco in dados:
        produto = session.query(Produto).filter_by(link=link).first()

        hoje = datetime.now(timezone.utc).date()
        registro_existente = (
            session.query(HistoricoPreco).filter_by(produto_id=produto.id, data_atualizacao=hoje).first()
        )

        if not registro_existente:
            objetos.append(HistoricoPreco(produto_id=produto.id, preco=preco, data_atualizacao=hoje))

    return objetos


def imagens(dados, session):
    """Processa dados de imagens, atualizando ou criando registros no banco de dados.

    Parâmetros:
    - dados (list of tuple): Lista de tuplas com (link, conteudo), onde `conteudo` pode ser bytes para atualizar imagens
    existentes ou int para criar novas com `produto_id`.
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
    """Salva dados em massa no banco de dados.

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
        print(f"Tipo de processamento '{tipo}' não é válido.")
        return

    session = Session()
    try:
        objetos = funcao_processamento(dados, session)
        session.bulk_save_objects(objetos)
        session.commit()
        print(f"{len(objetos)} registros de {tipo} salvos ou atualizados com sucesso.")
    except Exception as e:
        session.rollback()
        print(f"Erro ao salvar registros de {tipo} exexeçao {e}")
    finally:
        session.close()


def execute_today():
    with Session() as session:
        hoje = datetime.now().date()
        return session.query(HistoricoPreco).filter_by(data_atualizacao=hoje).first()


def get_link_produto():
    with Session() as session:
        return session.query(Produto).all()


def images_id():
    with Session() as session:
        return [image.produto_id for image in session.query(Imagem).all()]


def get_null_product_category():
    with Session() as session:
        return len(session.query(Produto).filter_by(categoria=None).all())


def get_count_products_without_images():
    with Session() as session:
        total_produtos = session.query(func.count(Produto.id)).scalar()

        produtos_com_imagens = session.query(func.count(Imagem.produto_id)).scalar()

        return total_produtos - produtos_com_imagens


def verificar_mudancas_preco():
    with Session() as session:
        # Subquery para encontrar a primeira e a última data de cada produto
        subquery = (
            session.query(
                HistoricoPreco.produto_id,
                func.min(HistoricoPreco.data_atualizacao).label("primeira_data"),
                func.max(HistoricoPreco.data_atualizacao).label("ultima_data"),
            )
            .group_by(HistoricoPreco.produto_id)
            .subquery()
        )

        # Query principal para obter os preços correspondentes à primeira e última data
        resultados = (
            session.query(
                HistoricoPreco.produto_id,
                func.first_value(HistoricoPreco.preco)
                .over(partition_by=HistoricoPreco.produto_id, order_by=HistoricoPreco.data_atualizacao)
                .label("preco_inicial"),
                func.first_value(HistoricoPreco.preco)
                .over(partition_by=HistoricoPreco.produto_id, order_by=HistoricoPreco.data_atualizacao.desc())
                .label("preco_final"),
                subquery.c.primeira_data,
                subquery.c.ultima_data,
            )
            .join(subquery, HistoricoPreco.produto_id == subquery.c.produto_id)
            .filter(and_(HistoricoPreco.data_atualizacao.in_([subquery.c.primeira_data, subquery.c.ultima_data])))
            .distinct()
        )

        # Filtrando apenas os produtos com mudança de preço
        return [r for r in resultados if r.preco_inicial != r.preco_final]


def price_change():
    mudancas = verificar_mudancas_preco()
    # for m in mudancas:
    #     print(f"Produto ID {m.produto_id} mudou de preço:")
    #     print(f"  De R$ {m.preco_inicial:.2f} em {m.primeira_data.strftime('%d/%m/%Y')}")
    #     print(f"  Para R$ {m.preco_final:.2f} em {m.ultima_data.strftime('%d/%m/%Y')}")
    #     print("--------------------")
    print(f"Total de produtos com mudança de preço: {len(mudancas)}")
