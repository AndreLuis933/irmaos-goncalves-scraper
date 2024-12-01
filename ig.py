# Importações
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

# Ativar PrettyColorPrinter
add_printer(True)

# Função utilitária: Gerenciador de contexto para suprimir stdout e stderr
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

# Download do chromedriver, com supressão de mensagens desnecessárias
with suppress_output():
    folder_path = "c:\\download2thisfolderchromedriver"
    chromedriver_path = download_undetected_chromedriver(
        folder_path,
        undetected=True,
        arm=False,
        force_update=True
    )

# Configuração do driver do Selenium
options = uc.ChromeOptions()
driver = uc.Chrome(
    options=options,
    driver_executable_path=chromedriver_path,
    headless=False
)
driver.maximize_window()

# Função auxiliar para capturar dados com a biblioteca `a_selenium2df`
def obter_dataframe(query="*"):
    """Obtém um DataFrame com base em elementos da página."""
    return get_df(driver, By, WebDriverWait, EC, queryselector=query, with_methods=True)

# Navegar para o site
driver.get('https://www.irmaosgoncalves.com.br/categoria/pet?p=1000')

# Início do tempo de execução
inicio = time.time()

# Aguarda o carregamento inicial da página
WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//select")))

# Captura inicial dos elementos da página
df = obter_dataframe("button, select, option")

# Interações iniciais na página
df.loc[
    (df.aa_localName == 'button') & (df.aa_innerText.str.contains('Aceitar', na=False))
].se_click.iloc[0]()
df.loc[df.aa_localName == 'select'].se_click.iloc[0]()
df.loc[
    (df.aa_localName == 'option') & (df.aa_innerText == 'Pimenta Bueno')
].se_click.iloc[0]()

# Espera breve para garantir que a página seja carregada corretamente
time.sleep(2)

# Captura dos elementos relevantes
df = obter_dataframe("img[title], div.text-xl.text-secondary.font-semibold.h-7, a.flex.justify-center.mt-5.cursor-pointer")

# Extração dos dados
nome_imagem = df.loc[
    (df.aa_localName == 'img') & (df.aa_outerHTML.str.contains('title', na=False))
].iloc[1:]

nome = nome_imagem['aa_title'].reset_index(drop=True)

imagem = nome_imagem['aa_src'].reset_index(drop=True)

preco = df.loc[
    (df.aa_localName == 'div') & (df.aa_classList.str.contains('text-xl text-secondary font-semibold h-7', na=False))
]['aa_innerText'].reset_index(drop=True)

link = df.loc[
    (df.aa_localName == 'a') & (df.aa_classList.str.contains('flex justify-center mt-5 cursor-pointer', na=False))
]['aa_href'].reset_index(drop=True)

# Combinar os dados em um único DataFrame
dados_completos = pd.concat([nome, imagem, preco, link], axis=1)

# Renomear as colunas para melhor identificação
dados_completos.columns = ['Nome', 'Imagem', 'Preço', 'Link']

# Fim do tempo de execução
fim = time.time()
tempo_execucao = fim - inicio
cookies = driver.get_cookies()
print(cookies)
# Exibir o tempo total de execução
print(f"O script levou {tempo_execucao:.2f} segundos para ser executado.")
dados_completos.to_excel('cachorro.xlsx', index=False)

