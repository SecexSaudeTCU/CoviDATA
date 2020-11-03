from os import path

import locale
import logging
import pandas as pd
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import salvar, consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.scrapper import Scraper


class TCE_PI_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas Estadual...')
        start_time = time.time()

        driver = self.__configurar_browser()
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

        # select_qtd = Select(driver.find_element_by_id('formDtContratos:dtContratos:j_id2'))
        select_qtd = Select(
            driver.find_element_by_xpath('/html/body/div[1]/div[2]/div[1]/div/div/form[2]/div/div[2]/select'))
        select_qtd.select_by_visible_text('100')
        time.sleep(5)

        df = self.__processar_paginas(driver, logger)
        persistir(df, 'tce', 'contratos', 'PI')

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def __processar_paginas(self, driver, logger):
        logger.info('Processando página 1...')
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        tabela = soup.find_all('table')[0]
        colunas = [th.get_text() for th in tabela.find_all('th')]
        linhas = []
        self.__processar_pagina(driver, linhas)
        i = 2

        try:
            while (True):
                btn_proximo = driver.find_element_by_class_name('ui-paginator-next')
                btn_proximo.click()
                logger.info('Processando página ' + str(i) + '...')
                time.sleep(2)
                self.__processar_pagina(driver, linhas)
                i += 1
        except ElementClickInterceptedException:
            logger.info('Processamento de páginas concluído.')
        df = pd.DataFrame(linhas, columns=colunas)

        return df

    def __processar_pagina(self, driver, linhas):
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        tabela = soup.find_all('table')[0]
        trs = tabela.find_all('tr')

        for tr in trs:
            tds = tr.find_all('td')
            valores = [td.contents[1].string for td in tds]
            if len(valores) > 0:
                linhas.append(valores)

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Piauí')
        logger.info('Consolidando as informações no layout padronizado...')
        start_time = time.time()

        consolidacoes = self.__consolidar_tce(data_extracao)

        logger.info("--- %s segundos ---" % (time.time() - start_time))

        return consolidacoes, False

    def __consolidar_tce(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'órgão',
                            consolidacao.DESPESA_DESCRICAO: 'objeto',
                            consolidacao.VALOR_CONTRATO: 'valor',
                            consolidacao.CONTRATADO_DESCRICAO: 'contratada',
                            consolidacao.CONTRATADO_CNPJ: 'doc contratada'}

        # Lê o arquivo "xlsx" de contratos baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'PI', 'tce', 'contratos.xls'), header=4)

        # Elimina a coluna objeto redundante
        df_original.drop(columns=['objeto'], inplace=True)
        df_original.rename(columns={'objeto.1': 'objeto'}, inplace=True)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_PI, 'PI', '',
                               data_extracao)
        return df

    def __configurar_browser(self):
        chromeOptions = webdriver.ChromeOptions()
        locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
        return driver
