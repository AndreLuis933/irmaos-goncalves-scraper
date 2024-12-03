from bs4 import BeautifulSoup
import requests
import logging
import json
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm import declarative_base
from datetime import datetime, timezone
from sqlalchemy import Date

Base = declarative_base()

class Produto(Base):
    __tablename__ = 'produtos'
    id = Column(Integer, primary_key=True)
    nome = Column(String, nullable=False)
    link = Column(String, unique=True, nullable=False)
    data_atualizacao = Column(Date, default=lambda: datetime.now(timezone.utc).date, onupdate=lambda: datetime.now(timezone.utc).date)

class HistoricoPreco(Base):
    __tablename__ = 'historico_precos'
    id = Column(Integer, primary_key=True)
    produto_id = Column(Integer, ForeignKey('produtos.id'), nullable=False)
    preco = Column(Float, nullable=False)
    data_registro = Column(Date, default=lambda: datetime.now(timezone.utc).date)

# Configuração do banco
DATABASE_URL = "sqlite:///produtos.db"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

# Criar sessão global
session = Session()

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurações globais
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def carregar_cookies():
    """
    Carrega os cookies de um arquivo JSON.
    """
    try:
        with open('cookies.json', 'r') as file:
            cookies = json.load(file)
            return cookies
    except Exception as e:
        logging.error(f"Erro ao carregar cookies: {e}")
        raise

def obter_resposta(url, headers, cookies):
    """
    Realiza a requisição HTTP para a URL fornecida.
    """
    try:
        response = requests.get(url, cookies=cookies, headers=headers)
        if response.status_code == 200:
            return response
        else:
            logging.error(f"Falha na requisição para {url}: Código {response.status_code}")
            return None
    except requests.RequestException as e:
        logging.error(f"Erro ao fazer requisição para {url}: {e}")
        return None

def extrair_dados(soup):
    """
    Extrai informações de nome, link, preço e imagem de um BeautifulSoup.
    """
    try:
        # Nomes e links dos produtos
        nome_link = [i.find('a') for i in soup.find_all(class_="h-[72px] text-ellipsis overflow-hidden cursor-pointer mt-2")]
        nome = [nome.text.strip() for nome in nome_link if nome]
        link = ['https://www.irmaosgoncalves.com.br' + link.get('href') for link in nome_link if link and link.get('href')]

        # Preços dos produtos
        preco = [a.text.strip() for a in soup.find_all('div', class_='text-xl text-secondary font-semibold h-7')]

        return nome, preco, link
    except Exception as e:
        logging.error(f"Erro ao extrair dados: {e}")
        return [], [], []

def verificar_tamanhos(nome, preco, link):
    """
    Verifica se todas as listas têm o mesmo tamanho.
    """
    list_sizes = [len(nome), len(preco), len(link)]
    if len(set(list_sizes)) != 1:
        logging.error(f"As listas têm tamanhos diferentes! Tamanhos: Nome={len(nome)}, Preço={len(preco)}, Link={len(link)}")
        raise ValueError("As listas extraídas não possuem o mesmo tamanho.")

def salvar_produtos_em_lote(session, produtos):
    """
    Insere ou atualiza produtos em lote.
    """
    novos_produtos = []
    for produto in produtos:
        existente = session.query(Produto).filter_by(link=produto['link']).first()
        if existente:
            existente.nome = produto['nome']
            existente.data_atualizacao = datetime.now(timezone.utc).date()
        else:
            novos_produtos.append(Produto(
                nome=produto['nome'],
                link=produto['link'],
                data_atualizacao=datetime.now(timezone.utc).date()
            ))
    if novos_produtos:
        session.bulk_save_objects(novos_produtos)
    session.commit()

def salvar_precos_em_lote(session, precos):
    """
    Insere preços no histórico em lote.
    """
    novos_precos = []
    for preco in precos:
        produto = session.query(Produto).filter_by(link=preco['link']).first()
        if produto:
            novos_precos.append(HistoricoPreco(
                produto_id=produto.id,
                preco=preco['preco'],
                data_registro=datetime.now(timezone.utc).date()
            ))
    session.bulk_save_objects(novos_precos)
    session.commit()

def obter_categorias_e_processar(url_base):
    """
    Obtém as categorias do site, gera as URLs e processa os dados de cada categoria.
    """
    try:
        # Requisição à página base para obter as categorias
        response = requests.get(url_base, headers=HEADERS)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Encontre os itens da lista suspensa
        lista = [i.get('href') for i in soup.find('ul').find_all('a')]

        categorias_filtradas = set()
        urls = []
        nomes_arquivos = []

        for categoria in lista:
            raiz = categoria.split('/categoria/')[1].split('/')[0]
            if raiz not in categorias_filtradas:
                categorias_filtradas.add(raiz)
                urls.append(url_base + categoria + '?p=10000')
                nomes_arquivos.append(raiz)

        # Processar as URLs geradas
        return urls, nomes_arquivos
    except Exception as e:
        logging.error(f"Erro ao obter categorias e processar: {e}")

def baixar_site(): 
    url_base = "https://www.irmaosgoncalves.com.br"
    urls, nomes_arquivos = obter_categorias_e_processar(url_base)

    cookies = carregar_cookies()
    produtos_para_salvar = []
    precos_para_salvar = []

    for url, nome in zip(urls, nomes_arquivos):
        try:
            logging.info(nome)
            response = obter_resposta(url, HEADERS, cookies)
            if response:
                soup = BeautifulSoup(response.content, 'html.parser')
                nome_prod, preco, link = extrair_dados(soup)
                verificar_tamanhos(nome_prod, preco, link)
                
                for n, p, l in zip(nome_prod, preco, link):
                    produtos_para_salvar.append({"nome": n, "link": l})
                    precos_para_salvar.append({
                    "produto_id": None,
                    "preco": float(p.replace("R$", "").replace(".", "").replace(",", ".").strip()),
                    "data_registro": datetime.now(timezone.utc).date(),
                    "link": l 
                })

        except Exception as e:
            logging.error(f"Erro ao processar {url}: {e}")

    # Salvar produtos em lote
    salvar_produtos_em_lote(session, produtos_para_salvar)

    # Salvar preços em lote
    salvar_precos_em_lote(session, precos_para_salvar)
    

    