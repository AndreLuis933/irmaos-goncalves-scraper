import logging

from sqlalchemy import and_, func

from database.connection import Session
from database.models import Cidade, HistoricoPreco, Produto

from .utils import gerenciador_transacao, inserir_com_conflito, obter_data_atual, obter_mapeamento_id


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
