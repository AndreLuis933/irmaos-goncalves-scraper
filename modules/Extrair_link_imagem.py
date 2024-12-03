import os
import sys
import time
from contextlib import contextmanager
import pandas as pd
import undetected_chromedriver as uc
from PrettyColorPrinter import add_printer
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from auto_download_undetected_chromedriver import download_undetected_chromedriver
from a_selenium2df import get_df
from datetime import datetime, timezone
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, LargeBinary
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
import pandas as pd
import time
#add_printer(True)


Base = declarative_base()

class Imagem(Base):
    __tablename__ = 'imagens'
    produto_id = Column(Integer, ForeignKey('produtos.id'), primary_key=True)  # Chave primária e estrangeira
    link_imagem = Column(String, nullable=False)  # URL da imagem
    conteudo = Column(LargeBinary, nullable=True)  # Conteúdo da imagem (BLOB)
    data_atualizacao = Column(Date, default=lambda: datetime.now(timezone.utc).date(), onupdate=lambda: datetime.now(timezone.utc).date())

    # Relacionamento com Produto
    produto = relationship("Produto", back_populates="imagem")


def extrair_link():
    @contextmanager
    def suppress_output():
        """Suprime stdout e stderr temporariamente."""
        with open(os.devnull, 'w') as devnull:
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = devnull
            sys.stderr = devnull
            try:
                yield
            finally:
                sys.stdout = old_stdout
                sys.stderr = old_stderr

    with suppress_output():
        folder_path = "c:\\download2thisfolderchromedriver"
        chromedriver_path = download_undetected_chromedriver(
            folder_path,
            undetected=True,
            arm=False,
            force_update=True
        )

    def criar_navegador():
        options = uc.ChromeOptions()
        driver = uc.Chrome(
            options=options,
            driver_executable_path=chromedriver_path,
            headless=False
        )
        driver.maximize_window()
        return driver

    def obter_dataframe(query="*"):
        """Obtém um DataFrame com base em elementos da página."""
        return get_df(driver, By, WebDriverWait, EC, queryselector=query, with_methods=True)

    # Configuração do banco de dados
    engine = create_engine("sqlite:///produtos.db")
    Session = sessionmaker(bind=engine)
    session = Session()


    # Consulta inicial à tabela 'produtos'
    query = "SELECT id, link FROM produtos"
    comparacao = pd.read_sql_query(query, engine)
    comparacao = comparacao.head(2)

    driver = criar_navegador()

    start_time = time.time()

    for index, row in comparacao.iterrows():
        link = row['link']
        driver.get(link)
        try:
            while "Too Many Requests" in driver.page_source:
                print("Erro 429 detectado: Muitas requisições enviadas.")
                time.sleep(10)
                driver.quit()
                driver = criar_navegador()
            try:
                while True:
                    # Esperar até que a imagem do produto esteja presente
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//img[contains(@src, 'produto')]"))
                    )
                    break
            except Exception as e:
                print('Nao achou o link da imagem')
                time.sleep(10)
                driver.quit()
                driver = criar_navegador()

            # Obter DataFrame de imagens da página
            df = obter_dataframe('img')

            imagem = df.loc[df.aa_src.str.contains('produto', na=False), 'aa_src'].iloc[0]
            comparacao.at[index, 'Imagem'] = imagem
            time.sleep(2)
        except Exception as e:
            print(f"Erro ao processar o link {link}: {e}")
            time.sleep(3)
            driver.quit()
            driver = criar_navegador()

    elapsed_time = time.time() - start_time
    print(f"Tempo para liberação: {elapsed_time:.2f} segundos")

    driver.quit()

    # Renomear as colunas do DataFrame para corresponder às do banco de dados
    comparacao['link'] = None
    comparacao.rename(
        columns={
            "id": "produto_id",
            "Imagem": "link_imagem",
            'link': 'conteudo' 
        },  
        inplace=True
    )
    # Adicionar a coluna de data_atualizacao
    comparacao['data_atualizacao'] = datetime.now(timezone.utc).date()
    comparacao.to_sql("imagens", con=engine, if_exists="replace", index=False)
    session.close()

# 0,2