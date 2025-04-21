from database.models import Cidade

from .utils import gerenciador_transacao


@gerenciador_transacao
def set_cidades(session, cidades):
    """Atualiza a tabela de cidades com a lista fornecida."""
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
