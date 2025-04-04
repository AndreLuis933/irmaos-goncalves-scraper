import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, LargeBinary, String, create_engine, text
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

load_dotenv()

Postegres = False

DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    if Postegres
    else "sqlite:///produtos.db"
)


# Criação do engine e da sessão
ENGINE = create_engine(DATABASE_URL)
Session = sessionmaker(bind=ENGINE)

Base = declarative_base()

"ProdutosIG"


class Produto(Base):
    __tablename__ = "produtos"
    id = Column(Integer, primary_key=True)
    nome = Column(String, nullable=False)
    link = Column(String, unique=True, nullable=False)
    categoria = Column(String, nullable=True)
    data_atualizacao = Column(
        Date,
        default=lambda: datetime.now(timezone.utc).date(),
        onupdate=lambda: datetime.now(timezone.utc).date(),
        nullable=False,
    )
    imagem = relationship("Imagem", back_populates="produto", uselist=False)
    historico_precos = relationship("HistoricoPreco", back_populates="produto", cascade="all, delete-orphan")


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


def init_db():
    Base.metadata.create_all(ENGINE)


if __name__ == "__main__":
    init_db()

