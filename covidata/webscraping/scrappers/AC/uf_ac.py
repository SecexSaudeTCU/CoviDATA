import time
from os import path

import pandas as pd
import requests
from bs4 import BeautifulSoup

import config
from persistencia.dao import persistir
from webscraping.downloader import FileDownloader
from webscraping.selenium.downloader import SeleniumDownloader


def extrair(url, informacao, indice):
    url = url
    colunas, linhas_df = extrair_tabela(url, indice)
    df = pd.DataFrame(linhas_df, columns=colunas)
    persistir(df, 'tce', informacao, 'AC')


def extrair_tabela(url, indice):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    tabela = soup.find_all('table')[indice]
    linhas = tabela.find_all('tr')
    titulos = linhas[0]
    titulos_colunas = titulos.find_all('td')
    colunas = [titulo_coluna.get_text() for titulo_coluna in titulos_colunas]
    linhas = linhas[1:]
    lista_linhas = []

    for linha in linhas:
        data = linha.find_all("td")
        nova_linha = [data[i].get_text() for i in range(len(colunas))]
        lista_linhas.append(nova_linha)

    return colunas, lista_linhas

class PortalTransparencia_RioBranco(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'Rio Branco'), url)

    def download(self):
        button = self.driver.find_element_by_partial_link_text('EXCEL')
        button.click()

        # Aguarda o download
        time.sleep(5)

        self.driver.close()
        self.driver.quit()

def main():
    extrair(url=config.url_tce_AC_contratos, informacao='contratos', indice=0)
    extrair(url=config.url_tce_AC_despesas, informacao='despesas', indice=0)
    extrair(url=config.url_tce_AC_despesas, informacao='dispensas', indice=1)
    extrair(url=config.url_tce_AC_contratos_municipios, informacao='contratos_municipios', indice=0)
    extrair(url=config.url_tce_AC_despesas_municipios, informacao='despesas_municipios', indice=0)
    extrair(url=config.url_tce_AC_despesas_municipios, informacao='dispensas_municipios', indice=1)

    pt_AC = FileDownloader(path.join(config.diretorio_dados, 'AC', 'portal_transparencia'), config.url_pt_AC,
                           'empenhos.csv')
    pt_AC.download()

    pt_RioBranco = PortalTransparencia_RioBranco(config.url_pt_RioBranco)
    pt_RioBranco.download()

