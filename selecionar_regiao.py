import os
import sys
from contextlib import contextmanager
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from auto_download_undetected_chromedriver import download_undetected_chromedriver
from a_selenium2df import get_df
import json
from datetime import datetime, timezone
import inquirer

regioes = [
    "Ariquemes", "Cacoal", "Guajará-Mirim", "Jaru", "Ji-Paraná",
    "Ouro Preto do Oeste", "Pimenta Bueno", "Porto Velho",
    "Rolim de Moura", "Vilhena"
]

questions = [
    inquirer.List('regiao',
                  message="Selecione uma região",
                  choices=regioes,
                  ),
]

answers = inquirer.prompt(questions)
regiao = answers['regiao']

@contextmanager
def suppress_output():
    """Suprime stdout e stderr temporariamente."""
    with open(os.devnull, "w") as devnull:
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
        folder_path, undetected=True, arm=False, force_update=True
    )


options = uc.ChromeOptions()
driver = uc.Chrome(
    options=options, driver_executable_path=chromedriver_path, headless=False
)
driver.maximize_window()


def obter_dataframe(query="*"):
    """Obtém um DataFrame com base em elementos da página."""
    return get_df(driver, By, WebDriverWait, EC, queryselector=query, with_methods=True)


driver.get("https://www.irmaosgoncalves.com.br/categoria/pet?p=1")


WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//select")))


df = obter_dataframe("button, select, option")


df.loc[df.aa_localName == "select"].se_click.iloc[0]()

# lista com as regioes ['Ariquemes', 'Cacoal', 'Guajará-Mirim', 'Jaru', 'Ji-Paraná', 'Ouro Preto do Oeste', 'Pimenta Bueno', 'Porto Velho', 'Rolim de Moura', 'Vilhena']

df.loc[
    (df.aa_localName == "option")
    & (df.aa_innerText == regiao)  # pode ser alterado para a sua região
].se_click.iloc[0]()

cookies = driver.get_cookies()

# so esta pegando o cookie app porque é o unico que importa para a escolha da regiao
novo = {}
for cookie in cookies:
    if cookie.get("name") == "app":
        expiry = cookie.get("expiry")

        readable_expiry = datetime.fromtimestamp(expiry, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )

        novo[cookie["name"]] = {
            "value": cookie.get("value"),
            "expiry": readable_expiry,
            "regiao": regiao,
        }

# Salvar cookies processados em um arquivo JSON
with open("cookies.json", "w", encoding="utf-8") as arquivo_json:
    json.dump(novo, arquivo_json, ensure_ascii=False, indent=4)

driver.quit()
