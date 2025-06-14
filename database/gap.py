from datetime import timedelta

from sqlalchemy import desc

from database import DisponibilidadeCidade, HistoricoPreco, LogExecucao, gerenciador_transacao
from utils.data import obter_data_atual


@gerenciador_transacao
def close_gap(session):
    """Finaliza registros antigos deixando o gap explícito"""
    ultima_data = session.query(LogExecucao.data_execucao).order_by(desc(LogExecucao.data_execucao)).first()

    if ultima_data:
        ultima_data = ultima_data[0]
        diff = obter_data_atual() - ultima_data
        if diff == timedelta(days=1) or diff == timedelta(days=0):
            print(f"Tudo certo")
            return

        session.query(DisponibilidadeCidade).filter(DisponibilidadeCidade.data_fim.is_(None)).update(
            {"data_fim": ultima_data},
            synchronize_session=False,
        )

        session.query(HistoricoPreco).filter(HistoricoPreco.data_fim.is_(None)).update(
            {"data_fim": ultima_data},
            synchronize_session=False,
        )

        print(
            f"✅ Gap explícito criado: {ultima_data + timedelta(days=1)} até {obter_data_atual() - timedelta(days=1)}"
        )
