import logging
import pickle
from collections import defaultdict
from datetime import datetime, timezone

from database.db_setup import ENGINE, Cidade, DisponibilidadeCidade, HistoricoPreco, Imagem, Produto, Session

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def main():
    with open("dados_produtos.pickle", "rb") as arquivo:
        resultados = pickle.load(arquivo)
    # resultados vem assim
    # [([[nome,link,categoria],[nome,link,categoria]], [[link,preco],[link,preco]],cidade), ([[nome,link,categoria],[nome,link,categoria]], [[link,preco],[link,preco]],cidade)]  # noqa: E501
    produtos_para_salvar = list(
        {
            produto[1]: produto  # Usa o link como chave
            for item in resultados
            for produto in item[0]
        }.values(),
    )

    mapeamento_links = defaultdict(list)

    for _, links_precos, cidade in resultados:
        for link, preco in links_precos:
            mapeamento_links[link].append((preco, cidade))

    lista_uniforme = []  # [link, preco]
    lista_variavel = []  # [link, preco, cidade]

    for link, precos_cidades in mapeamento_links.items():
        primeiro_preco = precos_cidades[0][0]
        if len(precos_cidades) == 1 or all(preco == primeiro_preco for preco, _ in precos_cidades[1:]):
            lista_uniforme.append([link, primeiro_preco])
        else:
            lista_variavel.extend([link, preco, cidade] for preco, cidade in precos_cidades)

    # produtos
    save_product(produtos_para_salvar)

    save_price(lista_uniforme, lista_variavel)

    save_disponibilidade(resultados)


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


main()
