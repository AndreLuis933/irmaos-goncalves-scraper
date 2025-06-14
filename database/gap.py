from datetime import timedelta

from utils.data import obter_data_atual

from .models import DisponibilidadeCidade, HistoricoPreco, LogExecucao
from .operations.utils import gerenciador_transacao, last_execution


@gerenciador_transacao
def close_gap(session):
    ultima_data = last_execution()
    if not ultima_data:
        return

    diff = obter_data_atual() - ultima_data
    if diff == timedelta(days=1) or diff == timedelta(days=0):
        print("Tudo certo")
        return

    session.query(DisponibilidadeCidade).filter(DisponibilidadeCidade.data_fim.is_(None)).update(
        {"data_fim": ultima_data},
        synchronize_session=False,
    )

    session.query(HistoricoPreco).filter(HistoricoPreco.data_fim.is_(None)).update(
        {"data_fim": ultima_data},
        synchronize_session=False,
    )

    print(f"✅ Gap explícito criado: {ultima_data + timedelta(days=1)} até {obter_data_atual() - timedelta(days=1)}")


@gerenciador_transacao
def log_execucao(session):
    ultima_data = last_execution()

    if ultima_data and obter_data_atual() == ultima_data:
        return
    session.add(LogExecucao(data_execucao=obter_data_atual()))
