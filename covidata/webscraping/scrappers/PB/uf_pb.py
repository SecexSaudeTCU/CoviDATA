import os
from os import path
import time
from datetime import datetime, timedelta
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import numpy as np
import pandas as pd

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader


# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_PB(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(str(config.diretorio_dados)[:-18], 'dados', 'PB', 'portal_transparencia', 'Paraiba'),
                         config.url_pt_PB,
                         browser_option='--start-maximized')

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        # Cria um objeto da class "WebDriverWait"
        wait = WebDriverWait(self.driver, 45)

        # Seleciona o botão "Exibir Relatório"
        element = wait.until(EC.visibility_of_element_located((By.XPATH,
                             '//*[@id="RPTRender_ctl08_ctl00"]')))
        self.driver.execute_script("arguments[0].click();", element)

        # On hold por 5 segundos
        time.sleep(5)

        # Seleciona o dropdown menu em forma de "disquete"
        self.driver.find_element_by_id('RPTRender_ctl09_ctl04_ctl00_ButtonImg').click()

        # Seleciona a opção "Excel" do dropdown menu salvando o arquivo "xlsx" contendo os dados de contratos COVID-19
        self.driver.find_element_by_xpath('//*[@id="RPTRender_ctl09_ctl04_ctl00_Menu"]/div[2]/a').click()

        # On hold por 5 segundos
        time.sleep(5)
        #
        # Lê o arquivo "xlsx" de contratos baixado como um objeto pandas DataFrame selecionando as linhas e colunas úteis
        df_contratos = pd.read_excel(path.join(str(config.diretorio_dados)[:-18], 'dados', 'PB', 'portal_transparencia', 'Paraiba',
                                     'ListaContratos.xlsx'), usecols=[0, 3, 4, 5, 6, 7, 9, 11], skiprows=list(range(4)))

        # Remove a última linha do objeto pandas DataFrame "df_contratos"
        df_contratos.drop(df_contratos.tail(1).index, inplace=True)

        # Converte as colunas de datas para objetos string
        for col in np.array(['Início', 'Final']):
            df_contratos[col] = df_contratos[col].apply(lambda x: x.strftime('%d/%m/%Y'))

        # Acrescenta a coluna "Nome Favorecido" ao objeto pandas DataFrame "df_contratos"
        df_contratos['Nome Favorecido'] = df_contratos['Contratado'].apply(lambda x: x.split(' - ')[1])
        # Acrescenta a coluna "CNPJ/CPF Favorecido" ao objeto pandas DataFrame "df_contratos"
        df_contratos['CNPJ/CPF Favorecido'] = df_contratos['Contratado'].apply(lambda x: x.split(' - ')[0])

        # Reordena as colunas do objeto pandas DataFrame "df_contratos"
        df_contratos = df_contratos[['Contrato', 'Nº Licitação', 'Início', 'Final', 'Órgão',
                                     'Nome Favorecido', 'CNPJ/CPF Favorecido', 'Objetivo',
                                     'Valor']]


        # Cria arquivo "xlsx" e aloca file handler de leitura para a variável "writer"
        with pd.ExcelWriter(path.join(str(config.diretorio_dados)[:-18], 'dados', 'PB', 'portal_transparencia',
                            'Paraiba', 'Dados_Portal_Transparencia_Paraiba.xlsx')) as writer:
            # Salva os dados de empenhos contidos em "df_contratos" na planilha "Contratos"
            df_contratos.to_excel(writer, sheet_name='Contratos', index=False)

        # Deleta o arquivo "xlsx" de nome "ListaContratos"
        os.unlink(path.join(str(config.diretorio_dados)[:-18], 'dados', 'PB', 'portal_transparencia', 'Paraiba', 'ListaContratos.xlsx'))



def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_PB = PortalTransparencia_PB()
    pt_PB.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
