import os

from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.sql import func

load_dotenv()

# se for usar postgres: o DATABASE_URL tem que ser definido no .env
DATABASE_URL = os.getenv("DATABASE_URL")
DATABASE_URL = None
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///ProdutosIG.db"


# Criação do engine e da sessão
ENGINE = create_engine(DATABASE_URL)
Session = sessionmaker(bind=ENGINE)

DATABASE_TYPE = ENGINE.dialect.name

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
        "DisponibilidadeCidade", back_populates="produto", cascade="all, delete-orphan"
    )


class Imagem(Base):
    __tablename__ = "imagens"
    produto_id = Column(Integer, ForeignKey("produtos.id"), primary_key=True)
    link_imagem = Column(String(1024), nullable=False)
    conteudo = Column(LargeBinary, nullable=True)
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
    data_atualizacao = Column(
        Date,
        default=func.current_date(),
        nullable=False,
    )

    # Relacionamentos
    produto = relationship("Produto", back_populates="historico_precos")
    cidade = relationship("Cidade", back_populates="historico_precos")

    __table_args__ = (
        UniqueConstraint("produto_id", "cidade_id", "data_atualizacao", name="uq_produto_cidade_data"),
        Index("idx_historico_data", data_atualizacao),
        Index("idx_historico_produto_cidade", produto_id, cidade_id),
    )


class DisponibilidadeCidade(Base):
    __tablename__ = "disponibilidade_cidades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    produto_id = Column(Integer, ForeignKey("produtos.id"), nullable=False)
    cidade_id = Column(Integer, ForeignKey("cidades.id"), nullable=False)
    data_disponibilidade = Column(
        Date,
        default=func.current_date(),
        nullable=False,
    )

    # Relacionamentos
    produto = relationship("Produto", back_populates="disponibilidade_cidades")
    cidade = relationship("Cidade", back_populates="disponibilidades")

    __table_args__ = (
        UniqueConstraint("produto_id", "cidade_id", "data_disponibilidade", name="uq_disponibilidade_cidade_data"),
        Index("idx_disponibilidade_produto_data", produto_id, data_disponibilidade),
        Index("idx_disponibilidade_cidade_data", cidade_id, data_disponibilidade),
    )


def init_db():
    Base.metadata.create_all(ENGINE)


if __name__ == "__main__" or DATABASE_TYPE == "sqlite":
    init_db()
