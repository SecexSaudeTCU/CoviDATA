import time
from os import path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from covidata import config
from covidata.persistencia.dao import persistir
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class TCE_MA(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MA', 'tce'), config.url_tce_MA)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        element = wait.until(EC.element_to_be_clickable((By.ID, 'z_n')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()


class PortalTransparencia_MA(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MA', 'portal_transparencia'), config.url_pt_MA)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()


def pt_sao_luis():
    page = requests.get(config.url_pt_SaoLuis)
    soup = BeautifulSoup(page.content, 'html.parser')
    tabela = soup.find_all('table')[0]
    tbody = tabela.find_all('tbody')[0]
    linhas = tbody.find_all('tr')
    titulos = tabela.find_all('th')
    colunas = ['Link contrato']
    colunas += [titulo.find_all('strong')[0].get_text() for titulo in titulos]
    lista_linhas = []

    for linha in linhas:
        data = linha.find_all("td")
        nova_linha = [data[0].next_element.attrs['href']]
        nova_linha += [data[i].get_text() for i in range(len(data))]
        lista_linhas.append(nova_linha)

    df = pd.DataFrame(lista_linhas, columns=colunas)
    persistir(df, 'portal_transparencia', 'contratacoes', 'MA', 'São Luís')


def main():
    print('Tribunal de contas estadual...')
    start_time = time.time()
    tce_ma = TCE_MA()
    tce_ma.download()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_ma = PortalTransparencia_MA()
    pt_ma.download()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência da capital...')
    start_time = time.time()
    pt_sao_luis()
    print("--- %s segundos ---" % (time.time() - start_time))


