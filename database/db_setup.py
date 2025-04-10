import os

from dotenv import load_dotenv
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, LargeBinary, String, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.sql import func

load_dotenv()

# se for usar postgres: o DATABASE_URL tem que ser definido no .env
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///ProdutosIG.db"

# Criação do engine e da sessão
ENGINE = create_engine(DATABASE_URL)
Session = sessionmaker(bind=ENGINE)

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


class HistoricoPreco(Base):
    __tablename__ = "historico_precos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    produto_id = Column(Integer, ForeignKey("produtos.id"), nullable=False)
    preco = Column(Float(precision=2), nullable=False)
    data_atualizacao = Column(
        Date,
        default=func.current_date(),
    )
    produto = relationship("Produto", back_populates="historico_precos")


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


def init_db():
    Base.metadata.create_all(ENGINE)


if __name__ == "__main__":
    init_db()

