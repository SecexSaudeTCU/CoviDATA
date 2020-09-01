import locale
import logging
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.scrapper import Scraper


class TCE_PI_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas Estadual...')

        driver = configurar_browser()
        driver.get(self.url)

        wait = WebDriverWait(driver, 45)

        # Seleciona a seta para baixo do menu dropdown de "Ano contrato"
        element = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="anoContrato"]/div[3]/span')))
        driver.execute_script("arguments[0].click();", element)

        # Seleciona o ano de 2020
        driver.find_element_by_id('anoContrato_1').click()

        # On hold por 5 segundos
        time.sleep(5)

        # Seleciona o botão "Pesquisar"
        driver.find_element_by_xpath('//*[@id="bPesquisar"]/span[2]').click()

        # On hold por 5 segundos
        time.sleep(5)

        select_qtd = Select(driver.find_element_by_id('formDtContratos:dtContratos:j_id21'))
        select_qtd.select_by_visible_text('100')
        time.sleep(5)

        logger.info('Processando página 1...')
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        tabela = soup.find_all('table')[0]
        colunas = [th.get_text() for th in tabela.find_all('th')]

        linhas = []
        self.processar_pagina(driver, linhas)
        i = 2

        try:
            while (True):
                btn_proximo = driver.find_element_by_class_name('ui-paginator-next')
                btn_proximo.click()
                logger.info('Processando página ' + str(i) + '...')
                time.sleep(2)
                self.processar_pagina(driver, linhas)
                i += 1
        except ElementClickInterceptedException:
            logger.info('Processamento de páginas concluído.')

        df = pd.DataFrame(linhas, columns=colunas)
        persistir(df, 'TCE', 'contratos', 'PI')

    def processar_pagina(self, driver, linhas):
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        tabela = soup.find_all('table')[0]
        trs = tabela.find_all('tr')

        for tr in trs:
            tds = tr.find_all('td')
            valores = [td.contents[1].string for td in tds]
            if len(valores) > 0:
                linhas.append(valores)

    def consolidar(self, data_extracao):
        pass


def configurar_browser():
    chromeOptions = webdriver.ChromeOptions()
    # chromeOptions.add_argument('--headless')
    locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
    return driver


TCE_PI_Scraper('https://sistemas.tce.pi.gov.br/muralcon/#').scrap()
