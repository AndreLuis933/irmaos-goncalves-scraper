import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

# Configuração da conexão
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///ProdutosIG.db")
ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(bind=ENGINE)
DATABASE_TYPE = ENGINE.dialect.name

