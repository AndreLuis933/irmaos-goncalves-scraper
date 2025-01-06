import os
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Date,
    ForeignKey,
    LargeBinary,
)
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from datetime import datetime, timezone

# Configuração do banco de dados
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "produtos.db")
ENGINE = create_engine(f"sqlite:///{DB_PATH}")
Session = sessionmaker(bind=ENGINE)

Base = declarative_base()


class Produto(Base):
    __tablename__ = "produtos"
    id = Column(Integer, primary_key=True)
    nome = Column(String, nullable=False)
    link = Column(String, unique=True, nullable=False)
    data_atualizacao = Column(
        Date,
        default=lambda: datetime.now(timezone.utc).date(),
        onupdate=lambda: datetime.now(timezone.utc).date(),
        nullable=False,
    )
    imagem = relationship("Imagem", back_populates="produto", uselist=False)
    historico_precos = relationship(
        "HistoricoPreco", back_populates="produto", cascade="all, delete-orphan"
    )


class HistoricoPreco(Base):
    __tablename__ = "historico_precos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    produto_id = Column(Integer, ForeignKey("produtos.id"), nullable=False)
    preco = Column(Float, nullable=False)
    data_atualizacao = Column(
        Date,
        default=lambda: datetime.now(timezone.utc).date(),
    )
    produto = relationship("Produto", back_populates="historico_precos")


class Imagem(Base):
    __tablename__ = "imagens"
    produto_id = Column(Integer, ForeignKey("produtos.id"), primary_key=True)
    link_imagem = Column(String, nullable=False)
    conteudo = Column(LargeBinary, nullable=True)
    data_atualizacao = Column(
        Date,
        default=lambda: datetime.now(timezone.utc).date(),
        onupdate=lambda: datetime.now(timezone.utc).date(),
        nullable=False,
    )
    produto = relationship("Produto", back_populates="imagem")
