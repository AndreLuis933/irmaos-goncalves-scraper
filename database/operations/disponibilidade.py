import logging

from database.models import Cidade, DisponibilidadeCidade, Produto

from .utils import gerenciador_transacao, inserir_com_conflito, obter_data_atual, obter_mapeamento_id


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
