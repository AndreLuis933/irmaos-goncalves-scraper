import logging

from sqlalchemy import and_, func

from database.connection import Session
from database.models import Cidade, HistoricoPreco, Produto
from utils.data import obter_data_atual

from .utils import atualizar_em_lotes, gerenciador_transacao, obter_mapeamento_id

logger = logging.getLogger(__name__)

@gerenciador_transacao
def salvar_preco(session, precos_uniformes, precos_variaveis):
    """Atualiza o histórico de preços de produtos no banco de dados.

    Processa preços uniformes (mesmo preço em todas as cidades) e variáveis (preços específicos
    por cidade), identificando e registrando apenas as mudanças efetivas. Fecha registros
    antigos e cria novos quando há alteração de preço, usando operações em lote para otimizar
    o desempenho com grandes volumes de dados.
    """
    if not precos_uniformes and not precos_variaveis:
        logger.info("Nenhum preço para salvar.")
        return

    hoje = obter_data_atual()

    todos_os_links = {p.link for p in precos_uniformes + precos_variaveis}
    todas_as_cidades = {p.cidade for p in precos_variaveis}

    link_to_id = obter_mapeamento_id(session, Produto, "link", todos_os_links)
    cidade_to_id = obter_mapeamento_id(session, Cidade, "nome", todas_as_cidades)
    cidade_padrao_id = 1

    novos_precos = {
        (link_to_id.get(p.link), cidade_padrao_id): p.preco for p in precos_uniformes if link_to_id.get(p.link)
    }
    novos_precos.update(
        {
            (link_to_id.get(p.link), cidade_to_id.get(p.cidade)): p.preco
            for p in precos_variaveis
            if link_to_id.get(p.link) and cidade_to_id.get(p.cidade)
        },
    )

    if not novos_precos:
        logger.info("Nenhum preço válido para processar.")
        return

    registros_ativos = (
        session.query(HistoricoPreco.produto_id, HistoricoPreco.cidade_id, HistoricoPreco.preco)
        .filter(HistoricoPreco.data_fim.is_(None))
        .all()
    )

    precos_atuais = {(r.produto_id, r.cidade_id): r.preco for r in registros_ativos}

    pares_novos_precos = set(novos_precos.keys())
    pares_precos_atuais = set(precos_atuais.keys())

    pares_novos = pares_novos_precos - pares_precos_atuais
    pares_removidos = pares_precos_atuais - pares_novos_precos

    pares_alterados = {
        par
        for par in pares_novos_precos.intersection(pares_precos_atuais)
        if abs(precos_atuais[par] - novos_precos[par]) > 0.01
    }

    pares_com_mudanca = list(pares_alterados.union(pares_removidos))

    valores_para_inserir = [
        HistoricoPreco(
            produto_id=produto_id,
            cidade_id=cidade_id,
            preco=novos_precos[(produto_id, cidade_id)],
            data_inicio=hoje,
            data_fim=None,
        )
        for produto_id, cidade_id in pares_novos.union(pares_alterados)
    ]

    alteracoes = 0
    if pares_com_mudanca:
        alteracoes += atualizar_em_lotes(session, pares_com_mudanca, HistoricoPreco)

    if valores_para_inserir:
        session.bulk_save_objects(valores_para_inserir)
        alteracoes += len(valores_para_inserir)

    logger.info(f"Total de alterações de preço: {alteracoes}")
    logger.info(f"{len(valores_para_inserir)} novos registros de preço inseridos.")
    logger.info(f"{len(pares_com_mudanca)} registros de preço fechados.")


def verificar_mudancas_preco():
    with Session() as session:
        # Subquery para encontrar a primeira e a última data de cada produto
        subquery = (
            session.query(
                HistoricoPreco.produto_id,
                func.min(HistoricoPreco.data_inicio).label("primeira_data"),
                func.max(HistoricoPreco.data_inicio).label("ultima_data"),
            )
            .group_by(HistoricoPreco.produto_id)
            .subquery()
        )

        # Query principal para obter os preços correspondentes à primeira e última data
        resultados = (
            session.query(
                HistoricoPreco.produto_id,
                func.first_value(HistoricoPreco.preco)
                .over(partition_by=HistoricoPreco.produto_id, order_by=HistoricoPreco.data_inicio)
                .label("preco_inicial"),
                func.first_value(HistoricoPreco.preco)
                .over(partition_by=HistoricoPreco.produto_id, order_by=HistoricoPreco.data_inicio.desc())
                .label("preco_final"),
                subquery.c.primeira_data,
                subquery.c.ultima_data,
            )
            .join(subquery, HistoricoPreco.produto_id == subquery.c.produto_id)
            .filter(and_(HistoricoPreco.data_inicio.in_([subquery.c.primeira_data, subquery.c.ultima_data])))
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
    logger.info(f"Total de produtos com mudança de preço: {len(mudancas)}")
