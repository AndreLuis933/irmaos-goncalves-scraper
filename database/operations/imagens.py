import logging

from sqlalchemy import func, not_

from database.connection import Session
from database.models import Imagem, Produto

from .utils import gerenciador_transacao


@gerenciador_transacao
def save_images(session,dados):
    if not dados:
        return

    # Verificar se é operação de atualização ou inserção
    primeiro_elemento = dados[0][0]
    operacao_atualizacao = isinstance(primeiro_elemento, bytes)

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
            img.produto_id for img in session.query(Imagem.produto_id).filter(Imagem.produto_id.in_(produto_ids)).all()
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


def images_id():
    with Session() as session:
        return [image.produto_id for image in session.query(Imagem).all()]


def get_count_products_without_images():
    with Session() as session:
        total_produtos = session.query(func.count(Produto.id)).scalar()

        produtos_com_imagens = session.query(func.count(Imagem.produto_id)).scalar()

        return total_produtos - produtos_com_imagens


def get_image_links():
    with Session() as session:
        imagens = (
            session.query(Imagem)
            .filter(Imagem.conteudo.is_(None))
            .filter(not_(Imagem.link_imagem.like("%removebg-preview%")))
            .all()
        )
        return [imagem.link_imagem for imagem in imagens]
