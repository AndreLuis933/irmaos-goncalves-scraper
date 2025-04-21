import logging
from datetime import datetime, timezone
from functools import wraps

import sqlalchemy

from database.connection import Session
from database.models import HistoricoPreco


def gerenciador_transacao(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            with Session() as session:
                result = func(session, *args, **kwargs)
                session.commit()
                return result
        except sqlalchemy.exc.IntegrityError:
            if "session" in locals():
                session.rollback()
            logging.exception(f"Erro de integridade ao executar {func.__name__}: ")
            return None
        except sqlalchemy.exc.SQLAlchemyError:
            if "session" in locals():
                session.rollback()
            logging.exception(f"Erro de banco de dados ao executar {func.__name__}: ")
            return None
        except ValueError:
            if "session" in locals():
                session.rollback()
            logging.exception(f"Erro de validação em {func.__name__}: ")
            return None
        except Exception:
            if "session" in locals():
                session.rollback()
            logging.exception(f"Erro inesperado em {func.__name__}: ")
            raise

    return wrapper


def inserir_com_conflito(session, tabela, valores, indices_conflito):
    if not valores:
        logging.info("Nenhum valor para inserir.")
        return 0

    dialect = session.bind.dialect.name

    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert

        stmt = insert(tabela).values(valores)
        stmt = stmt.on_conflict_do_nothing(index_elements=indices_conflito)
        result = session.execute(stmt)
        return result.rowcount

    table = tabela.__table__
    count = 0
    for valor in valores:
        stmt = table.insert().prefix_with("OR IGNORE").values(valor)
        session.execute(stmt)
        count += 1
    return count


def obter_data_atual():
    """Retorna data atual em UTC."""
    return datetime.now(timezone.utc).date()


def obter_mapeamento_id(session, modelo, campo_chave, valores):
    """Mapeia valores para IDs no banco de dados."""
    return {
        getattr(item, campo_chave): item.id
        for item in session.query(modelo.id, getattr(modelo, campo_chave))
        .filter(getattr(modelo, campo_chave).in_(valores))
        .all()
    }


def execute_today():
    with Session() as session:
        hoje = obter_data_atual()
        return session.query(HistoricoPreco).filter_by(data_atualizacao=hoje).first()


import pandas as pd
from database.connection import ENGINE


def get_dataframe(query):
    """Retorna um DataFrame do Pandas com os resultados da consulta SQL."""
    return pd.read_sql_query(query, ENGINE)
