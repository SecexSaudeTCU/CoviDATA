import datetime
import logging
import os
import time
from os import path

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.CE.consolidacao_CE import consolidar
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_Fortaleza(SeleniumDownloader):

    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'CE', 'portal_transparencia', 'Fortaleza'), url)

    def _executar(self):
        button = self.driver.find_element_by_id('download')
        button.click()


def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_CE = FileDownloader(path.join(config.diretorio_dados, 'CE', 'portal_transparencia'), config.url_pt_CE,
                           'gasto_covid_dados_abertos.xlsx')
    pt_CE.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Fortaleza = PortalTransparencia_Fortaleza(config.url_pt_Fortaleza)
    pt_Fortaleza.download()

    # Renomeia o arquivo
    diretorio = path.join(config.diretorio_dados, 'CE', 'portal_transparencia', 'Fortaleza')
    arquivo = os.listdir(diretorio)[0]
    os.rename(path.join(diretorio, arquivo), path.join(diretorio, 'despesas.csv'))

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
