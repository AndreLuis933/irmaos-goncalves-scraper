import logging

from database.connection import Session
from database.models import Produto
from utils.data import obter_data_atual

from .utils import gerenciador_transacao

logger = logging.getLogger(__name__)


@gerenciador_transacao
def salvar_produto(session, produtos):
    """Salva ou atualiza produtos no banco."""
    if not produtos:
        logger.info("Nenhum produto válido para inserir.")
        return

    hoje = obter_data_atual()

    links_recebidos = {p.link for p in produtos}
    produtos_atuais = {p.link: p for p in session.query(Produto).filter(Produto.link.in_(links_recebidos)).all()}

    links_para_inserir = links_recebidos - produtos_atuais.keys()
    links_para_atualizar = links_recebidos.intersection(produtos_atuais.keys())
    produtos_para_inserir = []

    for produto_info in produtos:
        if produto_info.link in links_para_inserir:
            produtos_para_inserir.append(
                Produto(
                    nome=produto_info.nome,
                    link=produto_info.link,
                    categoria=produto_info.categoria,
                    data_atualizacao=hoje,
                ),
            )
        elif produto_info.link in links_para_atualizar:
            produto_atual = produtos_atuais[produto_info.link]
            produto_atual.data_atualizacao = hoje
            if produto_atual.nome != produto_info.nome or (
                produto_info.categoria and produto_atual.categoria != produto_info.categoria
            ):
                produto_atual.nome = produto_info.nome
                if produto_info.categoria:
                    produto_atual.categoria = produto_info.categoria

    if produtos_para_inserir:
        session.bulk_save_objects(produtos_para_inserir)

    logger.info(f"{len(links_recebidos)} produtos atualizados ou inseridos com sucesso.")


def get_link_produto():
    with Session() as session:
        return session.query(Produto).all()


def get_null_product_category():
    with Session() as session:
        return {produto.id for produto in session.query(Produto.id).filter(Produto.categoria.is_(None)).all()}


def update_categoria(dados):
    """Atualiza a categoria de múltiplos produtos no banco de dados.

    Args:
        dados: Lista de tuplas no formato (id_produto, categoria) contendo
              o ID do produto e sua nova categoria.

    """
    with Session() as session:
        for id_produto, categoria in dados:
            produto = session.query(Produto).filter(Produto.id == id_produto).first()
            produto.categoria = categoria

        session.commit()
        logger.info(f"{len(dados)} categorias de produtos atualizadas com sucesso.")

def get_produtos_sem_categoria(limite):
    with Session() as session:
        produtos = (
            session.query(Produto.id, Produto.link)
            .filter(Produto.categoria.is_(None))
            .limit(limite)
            .all()
        )
        return {produto.link: produto.id for produto in produtos}
