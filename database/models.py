from sqlalchemy import Boolean, Column, Date, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .connection import ENGINE

Base = declarative_base()


class Produto(Base):
    __tablename__ = "produtos"
    id = Column(Integer, primary_key=True)
    nome = Column(String(255), nullable=False)
    link = Column(String(1024), unique=True, nullable=False)
    categoria = Column(String(255), nullable=True)
    data_atualizacao = Column(
        Date,
        default=func.current_date(),
        onupdate=func.current_date(),
        nullable=False,
    )
    imagem = relationship("Imagem", back_populates="produto", uselist=False)
    historico_precos = relationship("HistoricoPreco", back_populates="produto", cascade="all, delete-orphan")
    disponibilidade_cidades = relationship(
        "DisponibilidadeCidade",
        back_populates="produto",
        cascade="all, delete-orphan",
    )


class Imagem(Base):
    __tablename__ = "imagens"
    produto_id = Column(Integer, ForeignKey("produtos.id"), primary_key=True)
    link_imagem = Column(String(1024), nullable=False)
    data_atualizacao = Column(
        Date,
        default=func.current_date(),
        onupdate=func.current_date(),
        nullable=False,
    )
    produto = relationship("Produto", back_populates="imagem")


class Cidade(Base):
    __tablename__ = "cidades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(255), nullable=False, unique=True)

    disponibilidades = relationship("DisponibilidadeCidade", back_populates="cidade")
    historico_precos = relationship("HistoricoPreco", back_populates="cidade")


class HistoricoPreco(Base):
    __tablename__ = "historico_precos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    produto_id = Column(Integer, ForeignKey("produtos.id"), nullable=False)
    cidade_id = Column(Integer, ForeignKey("cidades.id"), nullable=False, default=1, server_default="1")
    preco = Column(Float(precision=2), nullable=False)
    data_inicio = Column(
        Date,
        default=func.current_date(),
        nullable=False,
    )
    data_fim = Column(
        Date,
        nullable=True,
    )

    produto = relationship("Produto", back_populates="historico_precos")
    cidade = relationship("Cidade", back_populates="historico_precos")

    __table_args__ = (UniqueConstraint("produto_id", "cidade_id", "data_inicio", name="uq_historico_preco_periodo"),)


class DisponibilidadeCidade(Base):
    __tablename__ = "disponibilidade_cidades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    produto_id = Column(Integer, ForeignKey("produtos.id"), nullable=False)
    cidade_id = Column(Integer, ForeignKey("cidades.id"), nullable=False)
    disponivel = Column(Boolean, nullable=False, default=True)
    data_inicio = Column(
        Date,
        default=func.current_date(),
        nullable=False,
    )
    data_fim = Column(
        Date,
        nullable=True,
    )

    produto = relationship("Produto", back_populates="disponibilidade_cidades")
    cidade = relationship("Cidade", back_populates="disponibilidades")

    __table_args__ = (
        UniqueConstraint("produto_id", "cidade_id", "data_inicio", name="uq_disponibilidade_cidade_periodo"),
    )


def init_db():
    """Inicializa o banco de dados criando todas as tabelas."""
    Base.metadata.create_all(ENGINE)
