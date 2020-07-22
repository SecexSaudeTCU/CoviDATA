from os import path
import logging
import time
from datetime import datetime
import requests

from bs4 import BeautifulSoup
import pandas as pd

from covidata import config
from covidata.persistencia.dao import persistir
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.scrappers.BA.consolidacao_BA import consolidar



def pt_BA():
    page = requests.get(config.url_pt_BA)
    soup = BeautifulSoup(page.content, 'html5lib')

    tabela = soup.find_all('table')[0]
    tbody = tabela.find_all('tbody')[0]
    titulos = tbody.contents[1]
    titulos = titulos.find_all('strong')

    nomes_colunas = ['Link para o contrato']
    nomes_colunas += [titulo.get_text() for titulo in titulos]

    linhas = []

    trs = tbody.find_all('tr')
    for i in range(2, len(trs)):
        tds = trs[i].find_all('td')
        links = tds[1].find_all('a')

        if len(links) > 0:
            link_contrato = links[0].attrs['href']
            linha = [link_contrato] + [td.get_text() for td in tds]
            linhas.append(linha)


    df = pd.DataFrame(linhas, columns=nomes_colunas)
    persistir(df, 'portal_transparencia', 'contratos', 'BA')


class TCE_BA(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'BA', 'tce'),
                         config.url_tce_BA,
                         browser_option='--start-maximized')

    def _executar(self):

        # On hold por 5 segundos
        time.sleep(5)

        # Seleciona o link referido pelo nome "Pressione..."
        self.driver.find_element_by_link_text('Pressione aqui para baixar os dados do painel em formato de planilha eletrônica').click()

        # On hold por 5 segundos
        time.sleep(5)


def main():
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')

    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_BA()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Tribunal de Contas estadual...')
    start_time = time.time()
    tce_BA = TCE_BA()
    tce_BA.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
