import logging

from database.models import Cidade, DisponibilidadeCidade, Produto
from utils.data import obter_data_atual

from .utils import atualizar_em_lotes, gerenciador_transacao, obter_mapeamento_id

logger = logging.getLogger(__name__)


@gerenciador_transacao
def salvar_disponibilidade(session, disponibilidades):
    """Atualiza o histórico de disponibilidade de produtos por cidade no banco de dados.

    Identifica mudanças na disponibilidade de produtos entre cidades, registrando quando
    produtos se tornam disponíveis ou indisponíveis. Utiliza operações de conjuntos e
    processamento em lote para otimizar a performance, fechando registros antigos e
    criando novos apenas quando há alterações efetivas.
    """
    if not disponibilidades:
        return

    hoje = obter_data_atual()

    links = {d.produto_link for d in disponibilidades}
    cidades = {d.cidade for d in disponibilidades}

    link_to_id = obter_mapeamento_id(session, Produto, "link", links)
    cidade_to_id = obter_mapeamento_id(session, Cidade, "nome", cidades)

    pares_disponiveis_hoje = {
        (link_to_id.get(d.produto_link), cidade_to_id.get(d.cidade))
        for d in disponibilidades
        if link_to_id.get(d.produto_link) and cidade_to_id.get(d.cidade)
    }

    registros_ativos = (
        session.query(DisponibilidadeCidade.produto_id, DisponibilidadeCidade.cidade_id)
        .filter(DisponibilidadeCidade.data_fim.is_(None), DisponibilidadeCidade.disponivel)
        .all()
    )

    pares_disponiveis_atual = {(r.produto_id, r.cidade_id) for r in registros_ativos}

    novos_disponiveis = pares_disponiveis_hoje - pares_disponiveis_atual
    novos_indisponiveis = pares_disponiveis_atual - pares_disponiveis_hoje

    valores_para_inserir = [
        DisponibilidadeCidade(
            produto_id=produto_id,
            cidade_id=cidade_id,
            disponivel=disponivel,
            data_inicio=hoje,
            data_fim=None,
        )
        for conjunto, disponivel in [(novos_disponiveis, True), (novos_indisponiveis, False)]
        for produto_id, cidade_id in conjunto
    ]
    alteracoes = 0

    pares_com_mudanca = list(novos_disponiveis.union(novos_indisponiveis))
    if pares_com_mudanca:
        alteracoes += atualizar_em_lotes(session, pares_com_mudanca, DisponibilidadeCidade)

    if valores_para_inserir:
        session.bulk_save_objects(valores_para_inserir)
        alteracoes += len(valores_para_inserir)

    logger.info(f"{len(novos_disponiveis)} produtos tornaram-se disponíveis.")
    logger.info(f"{len(novos_indisponiveis)} produtos tornaram-se indisponíveis.")
    logger.info(f"Total de alterações: {alteracoes}")
