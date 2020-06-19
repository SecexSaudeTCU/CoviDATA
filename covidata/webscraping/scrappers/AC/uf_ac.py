import time
from os import path

import pandas as pd

import config
from persistencia.dao import persistir
from webscraping.beautifulsoup.bs_util import extrair_tabela
from webscraping.downloader import FileDownloader
from webscraping.selenium.downloader import SeleniumDownloader


def __extrair(url, informacao, indice):
    url = url
    colunas, linhas_df = extrair_tabela(url, indice)
    df = pd.DataFrame(linhas_df, columns=colunas)
    persistir(df, 'tce', informacao, 'AC')


class PortalTransparencia_RioBranco(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'Rio Branco'), url)

    def _executar(self):
        button = self.driver.find_element_by_partial_link_text('EXCEL')
        button.click()


def main():
    print('Tribunal de Contas estadual...')
    start_time = time.time()
    __extrair(url=config.url_tce_AC_contratos, informacao='contratos', indice=0)
    __extrair(url=config.url_tce_AC_despesas, informacao='despesas', indice=0)
    __extrair(url=config.url_tce_AC_despesas, informacao='dispensas', indice=1)
    __extrair(url=config.url_tce_AC_contratos_municipios, informacao='contratos_municipios', indice=0)
    __extrair(url=config.url_tce_AC_despesas_municipios, informacao='despesas_municipios', indice=0)
    __extrair(url=config.url_tce_AC_despesas_municipios, informacao='dispensas_municipios', indice=1)
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_AC = FileDownloader(path.join(config.diretorio_dados, 'AC', 'portal_transparencia'), config.url_pt_AC,
                           'empenhos.csv')
    pt_AC.download()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência da capital...')
    start_time = time.time()
    pt_RioBranco = PortalTransparencia_RioBranco(config.url_pt_RioBranco)
    pt_RioBranco.download()
    print("--- %s segundos ---" % (time.time() - start_time))
