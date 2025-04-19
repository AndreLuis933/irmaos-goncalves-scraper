from __future__ import annotations

import logging
import pickle
from collections import defaultdict
from datetime import datetime, timezone
from functools import wraps
from typing import NamedTuple

import sqlalchemy

from database.db_setup import Cidade, DisponibilidadeCidade, HistoricoPreco, Produto, Session

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


class ProdutoInfo(NamedTuple):
    nome: str
    link: str
    categoria: str | None


class PrecoInfo(NamedTuple):
    link: str
    preco: float


class PrecoVariavel(NamedTuple):
    link: str
    preco: float
    cidade: str


class DisponibilidadeInfo(NamedTuple):
    produto_link: str
    cidade: str


class DadosProcessados(NamedTuple):
    produtos: list[ProdutoInfo]
    precos_uniformes: list[PrecoInfo]
    precos_variaveis: list[PrecoVariavel]
    disponibilidades: list[DisponibilidadeInfo]


def gerenciador_transacao(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            with Session() as session:
                result = func(session, *args, **kwargs)
                session.commit()
                return result
        except sqlalchemy.exc.IntegrityError:
            if "session" in locals():
                session.rollback()
            logging.exception(f"Erro de integridade ao executar {func.__name__}: ")
            return None
        except sqlalchemy.exc.SQLAlchemyError:
            if "session" in locals():
                session.rollback()
            logging.exception(f"Erro de banco de dados ao executar {func.__name__}: ")
            return None
        except ValueError:
            if "session" in locals():
                session.rollback()
            logging.exception(f"Erro de validação em {func.__name__}: ")
            return None
        except Exception:
            if "session" in locals():
                session.rollback()
            logging.exception(f"Erro inesperado em {func.__name__}: ")
            raise

    return wrapper


def inserir_com_conflito(session, tabela, valores, indices_conflito):
    if not valores:
        logging.info("Nenhum valor para inserir.")
        return 0

    dialect = session.bind.dialect.name

    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert

        stmt = insert(tabela).values(valores)
        stmt = stmt.on_conflict_do_nothing(index_elements=indices_conflito)
        result = session.execute(stmt)
        return result.rowcount

    table = tabela.__table__
    count = 0
    for valor in valores:
        stmt = table.insert().prefix_with("OR IGNORE").values(valor)
        session.execute(stmt)
        count += 1
    return count


def processar_dados_brutos(resultados):
    """Converte dados brutos em estruturas semânticas."""
    produtos: set[ProdutoInfo] = set()
    precos_por_link = defaultdict(list)
    disponibilidades = []

    # Extrair produtos e preços
    for produtos_raw, links_precos, cidade in resultados:
        for nome, link, categoria in produtos_raw:
            produtos.add(ProdutoInfo(nome=nome, link=link, categoria=categoria))
            disponibilidades.append(DisponibilidadeInfo(produto_link=link, cidade=cidade))

        for link, preco in links_precos:
            precos_por_link[link].append((preco, cidade))

    # Classificar preços uniformes e variáveis
    precos_uniformes: list[PrecoInfo] = []
    precos_variaveis: list[PrecoVariavel] = []

    for link, precos_cidades in precos_por_link.items():
        primeiro_preco = precos_cidades[0][0]
        if len(precos_cidades) == 1 or all(preco == primeiro_preco for preco, _ in precos_cidades[1:]):
            precos_uniformes.append(PrecoInfo(link=link, preco=primeiro_preco))
        else:
            for preco, cidade in precos_cidades:
                precos_variaveis.append(PrecoVariavel(link=link, preco=preco, cidade=cidade))

    return DadosProcessados(
        produtos=list(produtos),
        precos_uniformes=precos_uniformes,
        precos_variaveis=precos_variaveis,
        disponibilidades=disponibilidades,
    )


def obter_data_atual():
    """Retorna data atual em UTC."""
    return datetime.now(timezone.utc).date()


def obter_mapeamento_id(session, modelo, campo_chave, valores):
    """Mapeia valores para IDs no banco de dados."""
    return {
        getattr(item, campo_chave): item.id
        for item in session.query(modelo.id, getattr(modelo, campo_chave))
        .filter(getattr(modelo, campo_chave).in_(valores))
        .all()
    }


@gerenciador_transacao
def salvar_disponibilidade(session, disponibilidades):
    """Salva disponibilidades no banco."""
    if not disponibilidades:
        return

    hoje = obter_data_atual()

    # Obter mapeamentos
    links = {d.produto_link for d in disponibilidades}
    cidades = {d.cidade for d in disponibilidades}

    link_to_id = obter_mapeamento_id(session, Produto, "link", links)
    cidade_to_id = obter_mapeamento_id(session, Cidade, "nome", cidades)

    valores_para_inserir = [
        {
            "produto_id": link_to_id.get(d.produto_link),
            "cidade_id": cidade_to_id.get(d.cidade),
            "data_disponibilidade": hoje,
        }
        for d in disponibilidades
        if link_to_id.get(d.produto_link) and cidade_to_id.get(d.cidade)
    ]

    if not valores_para_inserir:
        logging.info("Nenhum produto válido para inserir.")
        return

    rows_affected = inserir_com_conflito(
        session,
        DisponibilidadeCidade,
        valores_para_inserir,
        ["produto_id", "cidade_id", "data_disponibilidade"],
    )

    logging.info(f"{rows_affected} registros de produtos disponíveis salvos com sucesso.")


@gerenciador_transacao
def salvar_preco(session, precos_uniformes, precos_variaveis):
    """Salva preços no banco."""
    if not precos_uniformes and not precos_variaveis:
        logging.info("Nenhum preço para salvar.")
        return

    hoje = obter_data_atual()
    valores_para_inserir = []

    todos_os_links = {p.link for p in precos_uniformes + precos_variaveis}
    todas_as_cidades = {p.cidade for p in precos_variaveis}

    link_to_id = obter_mapeamento_id(session, Produto, "link", todos_os_links)
    cidade_to_id = obter_mapeamento_id(session, Cidade, "nome", todas_as_cidades)

    # Transformar preços uniformes
    valores_para_inserir.extend(
        [
            {"produto_id": link_to_id.get(p.link), "preco": p.preco, "data_atualizacao": hoje}
            for p in precos_uniformes
            if link_to_id.get(p.link)
        ],
    )

    valores_para_inserir.extend(
        [
            {
                "produto_id": link_to_id.get(p.link),
                "cidade_id": cidade_to_id.get(p.cidade),
                "preco": p.preco,
                "data_atualizacao": hoje,
            }
            for p in precos_variaveis
            if link_to_id.get(p.link) and cidade_to_id.get(p.cidade)
        ],
    )

    if not valores_para_inserir:
        logging.info("Nenhum preço válido para inserir.")
        return

    rows_affected = inserir_com_conflito(
        session,
        HistoricoPreco,
        valores_para_inserir,
        ["produto_id", "cidade_id", "data_atualizacao"],
    )

    logging.info(f"{rows_affected} registros de preços salvos com sucesso.")


@gerenciador_transacao
def salvar_produto(session, produtos):
    """Salva ou atualiza produtos no banco."""
    if not produtos:
        logging.info("Nenhum produto válido para inserir.")
        return

    links_recebidos = {p.link for p in produtos}
    produtos_atuais = {p.link: p for p in session.query(Produto).filter(Produto.link.in_(links_recebidos)).all()}

    links_para_inserir = links_recebidos - produtos_atuais.keys()
    links_para_atualizar = links_recebidos.intersection(produtos_atuais.keys())
    produtos_para_inserir = []

    for produto_info in produtos:
        if produto_info.link in links_para_inserir:
            produtos_para_inserir.append(
                Produto(nome=produto_info.nome, link=produto_info.link, categoria=produto_info.categoria),
            )
        elif produto_info.link in links_para_atualizar:
            produto_atual = produtos_atuais[produto_info.link]
            if produto_atual.nome != produto_info.nome or (
                produto_info.categoria and produto_atual.categoria != produto_info.categoria
            ):
                produto_atual.nome = produto_info.nome
                if produto_info.categoria:
                    produto_atual.categoria = produto_info.categoria

    if produtos_para_inserir:
        session.bulk_save_objects(produtos_para_inserir)

    logging.info(f"{len(links_recebidos)} produtos atualizados ou inseridos com sucesso.")


def main():
    """Carrega dados e salva no banco."""
    with open("dados_produtos.pickle", "rb") as arquivo:
        resultados_brutos = pickle.load(arquivo)

    dados_processados = processar_dados_brutos(resultados_brutos)

    salvar_produto(dados_processados.produtos)
    salvar_preco(dados_processados.precos_uniformes, dados_processados.precos_variaveis)
    salvar_disponibilidade(dados_processados.disponibilidades)


main()
