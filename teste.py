import pandas as pd
from  a_selenium2df import get_df
import undetected_chromedriver as uc
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from auto_download_undetected_chromedriver import download_undetected_chromedriver
import os
import sys
from contextlib import contextmanager
import time
from PrettyColorPrinter import add_printer
add_printer(True)


# Criar um gerenciador de contexto para suprimir stdout e stderr
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


# Suprimir mensagens durante o download
with suppress_output():
    folder_path = "c:\\download2thisfolderchromedriver"
    chromedriver_path = download_undetected_chromedriver(
        folder_path,
        undetected=True,
        arm=False,
        force_update=True  # Faz download repetido
    )

options = uc.ChromeOptions()
driver = uc.Chrome(
    options=options,
    driver_executable_path=chromedriver_path,
    headless=False
)

driver.maximize_window()

def g(q="*",):
    return get_df(driver, By, WebDriverWait, EC, queryselector=q, with_methods=True, )

inicio_total = time.time()

# Abrir a URL
inicio = time.time()
driver.get('https://www.irmaosgoncalves.com.br/categoria/pet?p=1000')
fim = time.time()
print(f"Abertura da página levou {fim - inicio:.2f} segundos.")

# Esperar o elemento ser clicável
inicio = time.time()
WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//select")))
fim = time.time()
print(f"Espera do elemento clicável levou {fim - inicio:.2f} segundos.")

# Obter o DataFrame inicial
inicio = time.time()
df = g("button, select, option")
fim = time.time()
print(f"Extração inicial de elementos levou {fim - inicio:.2f} segundos.")

# Realizar cliques nos elementos
inicio = time.time()
df.loc[(df.aa_localName == 'button') & (df.aa_innerText.str.contains('Aceitar',na=False))].se_click.iloc[0]()
df.loc[df.aa_localName == 'select'].se_click.iloc[0]()
df.loc[(df.aa_localName == 'option') & (df.aa_innerText == 'Pimenta Bueno')].se_click.iloc[0]()
fim = time.time()
print(f"Interações com elementos levaram {fim - inicio:.2f} segundos.")

# Aguardar o carregamento adicional (simulado com time.sleep)
inicio = time.time()
time.sleep(2)
fim = time.time()
print(f"Espera adicional (time.sleep) levou {fim - inicio:.2f} segundos.")

# Obter o DataFrame final
inicio = time.time()
df = g("img[title], div.text-xl.text-secondary.font-semibold.h-7, a.flex.justify-center.mt-5.cursor-pointer")
fim = time.time()
print(f"Extração final de elementos levou {fim - inicio:.2f} segundos.")

# Processar os dados
inicio = time.time()
nome_imagem =df.loc[(df.aa_localName == 'img') & (df.aa_outerHTML.str.contains('title',na=False))].iloc[1:]
nome = nome_imagem['aa_title']
imagem = nome_imagem['aa_src']
preco = df.loc[(df.aa_localName == 'div') & (df.aa_classList.str.contains('text-xl text-secondary font-semibold h-7',na=False))]['aa_innerText']
link = df.loc[(df.aa_localName == 'a') & (df.aa_classList.str.contains('flex justify-center mt-5 cursor-pointer',na=False))]['aa_href']

nome = nome.reset_index(drop=True)
imagem = imagem.reset_index(drop=True)
preco = preco.reset_index(drop=True)
link = link.reset_index(drop=True)

# Concatenar em um DataFrame
junto_df = pd.concat([nome, imagem, preco, link], axis=1)

# Renomear as colunas para identificação
junto_df.columns = ['Nome', 'Imagem', 'Preço', 'Link']
fim = time.time()
print(f"Processamento de dados levou {fim - inicio:.2f} segundos.")

# Tempo total
fim_total = time.time()
print(f"O script levou {fim_total - inicio_total:.2f} segundos para ser executado.")


