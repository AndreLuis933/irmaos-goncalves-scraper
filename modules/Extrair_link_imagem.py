import os
import sys
import time
from contextlib import contextmanager
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from auto_download_undetected_chromedriver import download_undetected_chromedriver
from a_selenium2df import get_df
from datetime import datetime, timezone
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, LargeBinary
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from math import prod
from PoorMansHeadless import FakeHeadless
from a_cv_imwrite_imread_plus import open_image_in_cv
from cv2imshow.cv2imshow import cv2_imshow_single

Base = declarative_base()

class Imagem(Base):
    __tablename__ = 'imagens'
    produto_id = Column(Integer, ForeignKey('produtos.id'), primary_key=True)  # Chave primária e estrangeira
    link_imagem = Column(String, nullable=False)  # URL da imagem
    conteudo = Column(LargeBinary, nullable=True)  # Conteúdo da imagem (BLOB)
    data_atualizacao = Column(Date, default=lambda: datetime.now(timezone.utc).date(), onupdate=lambda: datetime.now(timezone.utc).date())

    # Relacionamento com Produto
    produto = relationship("Produto", back_populates="imagem")

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

def get_hwnd(driver):
    while True:
        try:
            allhwnds=[x for x in FakeHeadless.get_all_windows_with_handle() if x.pid == driver.browser_pid]
            return sorted(allhwnds, key=lambda x: prod(x.dim_win), reverse=True)[0].hwnd
        except Exception:
            continue

def extrair_link():
    options = uc.ChromeOptions()
    driver = uc.Chrome(
        options=options,
        driver_executable_path=chromedriver_path,
        headless=False
    )
    hwnd=get_hwnd(driver)
    driverheadless=FakeHeadless(hwnd)
    #driverheadless.start_headless_mode(width=None, height=None, distance_from_taskbar=1)
    screenshot1=lambda: cv2_imshow_single(open_image_in_cv(driver.get_screenshot_as_png()),title='sele1',killkeys="ctrl+alt+q")

    driver.maximize_window()

    def obter_dataframe(query="*"):
        """Obtém um DataFrame com base em elementos da página."""
        return get_df(driver, By, WebDriverWait, EC, queryselector=query, with_methods=True)

    engine = create_engine("sqlite:///produtos.db")
    Session = sessionmaker(bind=engine)
    session = Session()

    query = """
    SELECT id, link 
    FROM produtos
    WHERE id NOT IN (SELECT produto_id FROM imagens);
    """
    comparacao = pd.read_sql_query(query, engine)
    
    comparacao.rename(columns={"id": "produto_id",},inplace=True)
    start_time = time.time()

    #comparacao = comparacao.head(20)

    for index, row in comparacao.iterrows():
        link = row['link']
        driver.get(link)
        
        while "Too Many Requests" in driver.page_source:
            print("Erro 429 detectado: Muitas requisições enviadas.")
            time.sleep(10)
            driver.get(link)

        tentativas = 0
        max_tentativas = 5  # Limite de tentativas

        while tentativas < max_tentativas:
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//img[contains(@src, 'produto')]"))
                )
                break  # Sai do loop se a imagem aparecer
            except Exception as e:
                print(f"Tentativa {tentativas + 1}: Não achou o link da imagem")
                tentativas += 1
                time.sleep(5)  # Aguarda antes de tentar novamente
                driver.get(link)  # Recarrega a página
        else:
            print("Falha ao carregar a imagem após várias tentativas.")
            
        df = obter_dataframe('img')
        imagem = df.loc[df.aa_src.str.contains('produto', na=False), 'aa_src'].iloc[0]
        comparacao.at[index, 'link_imagem'] = imagem
        
        linha = comparacao.loc[[index]].copy()
        linha['data_atualizacao'] = datetime.now(timezone.utc).date()
        
        linha.drop(columns=['link']).to_sql(
            "imagens", con=engine, if_exists="append", index=False
        )

        time.sleep(2)

    elapsed_time = time.time() - start_time
    print(f"Tempo Que ficou trabalhando: {elapsed_time:.2f} segundos")

    driver.quit()
    session.close()


# 0,2 imagem por segundo