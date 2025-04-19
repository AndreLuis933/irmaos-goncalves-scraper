def save_images1(dados):
    """Processa dados de imagens, atualizando ou criando registros no banco de dados.

    Parâmetros:
    - dados (list of tuple): Lista de tuplas com (link, conteudo), onde `conteudo` pode ser bytes para atualizar imagens
    existentes ou int para criar novas com `produto_id`.

    """
    try:
        with Session() as session:
            objetos = []
            for conteudo, link in dados:
                if isinstance(conteudo, bytes):
                    imagem = session.query(Imagem).filter_by(link_imagem=link).first()
                    imagem.conteudo = conteudo
                elif isinstance(conteudo, int):
                    objetos.append(Imagem(produto_id=conteudo, link_imagem=link))

            session.bulk_save_objects(objetos)
            session.commit()
            logging.info(f"{len(objetos)} registros de imagens salvos ou atualizados com sucesso.")
    except Exception as e:
        session.rollback()
        logging.info(f"Erro ao salvar registros de imagens exexeçao {e}")


def save_price1(dados):
    try:
        with Session() as session:
            hoje = datetime.now(timezone.utc).date()

            # Extrair links e criar mapeamento produtos
            links = [link for link, _ in dados]
            link_to_id = {
                p.link: p.id for p in session.query(Produto.id, Produto.link).filter(Produto.link.in_(links)).all()
            }

            # Obter IDs de produtos que já têm registros hoje
            produtos_com_registro = {
                r.produto_id
                for r in session.query(HistoricoPreco.produto_id)
                .filter(
                    HistoricoPreco.produto_id.in_(list(link_to_id.values())),
                    HistoricoPreco.data_atualizacao == hoje,
                )
                .all()
            }

            # Criar objetos para inserção, filtrando produtos não encontrados e já registrados
            objetos = [
                HistoricoPreco(produto_id=link_to_id.get(link), preco=preco, data_atualizacao=hoje)
                for link, preco in dados
                if link_to_id.get(link) and link_to_id.get(link) not in produtos_com_registro
            ]

            if objetos:
                session.bulk_save_objects(objetos)
                session.commit()
                logging.info(f"{len(objetos)} registros de preços salvos com sucesso.")
            else:
                logging.info("Nenhum novo registro de preço para salvar hoje.")

    except Exception as e:
        session.rollback()
        logging.info(f"Erro ao salvar registros de preços: {e}")
