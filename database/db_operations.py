import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import and_, func, not_

from .db_setup import ENGINE, Cidade, DisponibilidadeCidade, HistoricoPreco, Imagem, Produto, Session


def set_cidades(cidades):
    """Atualiza a tabela de cidades com a lista fornecida.

    Args:
        cidades (list): Lista de strings com nomes de cidades

    """
    with Session() as session:
        tabela_vazia = session.query(Cidade).count() == 0

        # Se a tabela estiver vazia, adiciona "Sem Cidade" primeiro
        if tabela_vazia:
            sem_cidade = Cidade(nome="Sem Cidade")
            session.add(sem_cidade)
            session.commit()

        # Busca as cidades existentes
        cidades_existentes = {cidade.nome for cidade in session.query(Cidade).all()}

        # Adiciona apenas cidades que não existem
        cidades_para_adicionar = [
            Cidade(nome=nome_cidade) for nome_cidade in cidades if nome_cidade not in cidades_existentes
        ]

        if cidades_para_adicionar:
            session.add_all(cidades_para_adicionar)
            session.commit()


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


def save_disponibilidade(dados):
    if not dados:
        return
    try:
        with Session() as session:
            hoje = datetime.now(timezone.utc).date()
            links = [produto[1] for produtos in dados for produto in produtos[0]]
            cidades = [cidade[2] for cidade in dados]
            link_to_id = {
                p.link: p.id for p in session.query(Produto.id, Produto.link).filter(Produto.link.in_(links)).all()
            }
            cidade_to_id = {
                c.nome: c.id for c in session.query(Cidade.id, Cidade.nome).filter(Cidade.nome.in_(cidades)).all()
            }

            valores_para_inserir = [
                {
                    "produto_id": link_to_id.get(produto[1]),
                    "cidade_id": cidade_to_id.get(cidade),
                    "data_disponibilidade": hoje,
                }
                for produtos, _, cidade in dados
                for produto in produtos
                if link_to_id.get(produto[1])
            ]

            if not valores_para_inserir:
                logging.info("Nenhum produto válido para inserir.")
                return

            dialect = session.bind.dialect.name

            if dialect == "postgresql":
                from sqlalchemy.dialects.postgresql import insert

                stmt = insert(DisponibilidadeCidade).values(valores_para_inserir)
                stmt = stmt.on_conflict_do_nothing(index_elements=["produto_id", "cidade_id", "data_disponibilidade"])
                result = session.execute(stmt)
                session.commit()
                logging.info(f"{result.rowcount} registros de produtos disponives salvos com sucesso.")
            else:
                # Versão para outros bancos
                table = DisponibilidadeCidade.__table__
                for valor in valores_para_inserir:
                    stmt = table.insert().prefix_with("OR IGNORE").values(valor)
                    session.execute(stmt)

                logging.info("Registros de produtos disponives salvos com sucesso.")

                session.commit()

    except Exception as e:
        session.rollback()
        logging.info(f"Erro ao salvar registros de produtos disponives: {e}")


def save_price(lista_uniforme, lista_variavel):
    if not lista_uniforme:
        return
    try:
        with Session() as session:
            hoje = datetime.now(timezone.utc).date()

            # Extrair links e criar mapeamento produtos
            links = [link for link, _ in lista_uniforme]
            link_to_id = {
                p.link: p.id for p in session.query(Produto.id, Produto.link).filter(Produto.link.in_(links)).all()
            }

            # Preparar dados para inserção
            valores_para_inserir = [
                {"produto_id": link_to_id.get(link), "preco": preco, "data_atualizacao": hoje}
                for link, preco in lista_uniforme
                if link_to_id.get(link)
            ]

            cidades = [cidade for _, _, cidade in lista_variavel]
            links = [link for link, _, _ in lista_variavel]
            link_to_id = {
                p.link: p.id for p in session.query(Produto.id, Produto.link).filter(Produto.link.in_(links)).all()
            }
            cidade_to_id = {
                c.nome: c.id for c in session.query(Cidade.id, Cidade.nome).filter(Cidade.nome.in_(cidades)).all()
            }
            valores_para_inserir.extend(
                [
                    {
                        "produto_id": link_to_id.get(link),
                        "cidade_id": cidade_to_id.get(cidade),
                        "preco": preco,
                        "data_atualizacao": hoje,
                    }
                    for link, preco, cidade in lista_variavel
                    if link_to_id.get(link)
                ],
            )

            if not valores_para_inserir:
                logging.info("Nenhum produto válido para inserir.")
                return

            dialect = session.bind.dialect.name

            if dialect == "postgresql":
                from sqlalchemy.dialects.postgresql import insert

                stmt = insert(HistoricoPreco).values(valores_para_inserir)
                stmt = stmt.on_conflict_do_nothing(index_elements=["produto_id", "cidade_id", "data_atualizacao"])
                result = session.execute(stmt)
                session.commit()
                logging.info(f"{result.rowcount} registros de preços salvos com sucesso.")
            else:
                # Versão para outros bancos
                table = HistoricoPreco.__table__
                for valor in valores_para_inserir:
                    stmt = table.insert().prefix_with("OR IGNORE").values(valor)
                    session.execute(stmt)

                session.commit()
                logging.info("Registros de preços salvos com sucesso.")

    except Exception as e:
        session.rollback()
        logging.info(f"Erro ao salvar registros de preços: {e}")


def save_product(dados):
    if not dados:
        logging.info("Nenhum produto valido para inserir.")
        return

    try:
        with Session() as session:
            produtos_atuais = {p.link: p for p in session.query(Produto).all()}
            links_recebidos = {link for _, link, _ in dados}

            # Identificar produtos para inserir e atualizar
            links_para_inserir = links_recebidos - produtos_atuais.keys()
            links_para_atualizar = links_recebidos.intersection(produtos_atuais.keys())
            produtos_para_inserir = []

            # Processar atualizações e inserções
            for nome, link, categoria in dados:
                if link in links_para_inserir:
                    produtos_para_inserir.append(Produto(nome=nome, link=link, categoria=categoria))
                elif link in links_para_atualizar:
                    produto = produtos_atuais[link]
                    if produto.nome != nome or (categoria and produto.categoria != categoria):
                        produto.nome = nome
                        if categoria:
                            produto.categoria = categoria

            if produtos_para_inserir:
                session.bulk_save_objects(produtos_para_inserir)

            session.commit()
            logging.info(f"{len(links_recebidos)} produtos atualizados ou inseridos com sucesso.")

    except Exception as e:
        session.rollback()
        logging.info(f"Erro ao processar produtos: {e}")


def save_images(dados):
    if not dados:
        return

    # Verificar se é operação de atualização ou inserção
    primeiro_elemento = dados[0][0]
    operacao_atualizacao = isinstance(primeiro_elemento, bytes)

    try:
        with Session() as session:
            contador = 0

            if operacao_atualizacao:
                links = [link for _, link in dados]
                imagens = session.query(Imagem).filter(Imagem.link_imagem.in_(links)).all()

                # Criar mapeamento de link para imagem
                imagens_por_link = {img.link_imagem: img for img in imagens}

                # Atualizar conteúdo em lote
                for conteudo, link in dados:
                    if link in imagens_por_link:
                        imagens_por_link[link].conteudo = conteudo
                        contador += 1
            else:
                produto_ids = [produto_id for produto_id, _ in dados]

                # Verificar quais produtos já têm imagens
                produtos_com_imagem = {
                    img.produto_id
                    for img in session.query(Imagem.produto_id).filter(Imagem.produto_id.in_(produto_ids)).all()
                }

                # Filtrar apenas produtos sem imagem
                objetos = [
                    Imagem(produto_id=produto_id, link_imagem=link)
                    for produto_id, link in dados
                    if produto_id not in produtos_com_imagem
                ]

                if objetos:
                    session.bulk_save_objects(objetos)
                    contador = len(objetos)

            session.commit()
            logging.info(f"{contador} registros de imagens salvos ou atualizados com sucesso.")

    except Exception as e:
        if "session" in locals() and session.is_active:
            session.rollback()
        logging.info(f"Erro ao salvar registros de imagens: {e}")


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
        return session.query(Produto).filter(Produto.categoria.is_(None)).count()


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
    logging.info(f"Total de produtos com mudança de preço: {len(mudancas)}")

