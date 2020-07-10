import os
from os import path
import time
from datetime import datetime
import logging

import pandas as pd

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.scrappers.RN.consolidacao_RN import consolidar



# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_RN(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte'),
                         config.url_pt_RN,
                         browser_option='--start-maximized')

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        # Seleciona o botão em forma de planilha do Excel
        self.driver.find_element_by_xpath('//*[@id="DataTables_Table_0_wrapper"]/div[1]/button[2]/span/i').click()



# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_Natal(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'Natal'),
                         config.url_pt_Natal)

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        # On hold por 3 segundos
        time.sleep(3)

        # Seleciona o botão em forma de planilha do Excel
        self.driver.find_element_by_xpath('//*[@id="DataTables_Table_0_wrapper"]/div[1]/button[2]/span').click()



def main():
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_RN = PortalTransparencia_RN()
    pt_RN.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Natal = PortalTransparencia_Natal()
    pt_Natal.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
