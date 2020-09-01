import os
from os import path
import time
from datetime import datetime
import logging

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.scrappers.PI.consolidacao_PI import consolidar



# Define a classe referida como herdeira da class "SeleniumDownloader"
class TCE_Piaui(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'PI', 'tce',),
                         config.url_tce_PI,
                         browser_option='--start-maximized'
                         )

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        wait = WebDriverWait(self.driver, 45)

        # Seleciona a seta para baixo do menu dropdown de "Ano contrato"
        element = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="anoContrato"]/div[3]/span')))
        self.driver.execute_script("arguments[0].click();", element)

        # Seleciona o ano de 2020
        self.driver.find_element_by_id('anoContrato_1').click()

        # On hold por 5 segundos
        time.sleep(5)

        # Seleciona o botão "Pesquisar"
        self.driver.find_element_by_xpath('//*[@id="bPesquisar"]/span[2]').click()

        # On hold por 5 segundos
        time.sleep(5)

        # Seleciona o símbolo na forma de planilha Excel
        #self.driver.find_element_by_xpath('//*[@id="formDtContratos:dtContratos_paginator_top"]/div/a/span').click()

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')


def main():
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')

    logger.info('Tribunal de Contas Estadual...')
    start_time = time.time()
    tce_piaui = TCE_Piaui()
    tce_piaui.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

main()