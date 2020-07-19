import os
from os import path
import logging
import time
import datetime
import requests

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
from bs4 import BeautifulSoup

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.downloader import download
from covidata.webscraping.scrappers.MT.consolidacao_MT import consolidar



class PortalTransparencia_MT(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MT', 'portal_transparencia'),
                         config.url_pt_MT)

    def _executar(self):

        # Cria um objeto da class "WebDriverWait"
        wait = WebDriverWait(self.driver, 45)

        # Entra no iframe de id "iframeInformacoes"
        iframe = wait.until(EC.visibility_of_element_located((By.ID, 'iframeInformacoes')))
        self.driver.switch_to.frame(iframe)

        # Seleciona o botão "Versão Completa"
        self.driver.find_element_by_xpath('/html/body/div/a').click()

        # On hold por 5 segundos
        time.sleep(5)

        # Seleciona o menu dropdown "DOWNLOAD DOS DADOS"
        self.driver.find_element_by_xpath('/html/body/div/div[5]/button').click()

        # Seleciona a opção "Excel"
        self.driver.find_element_by_xpath('/html/body/div/div[5]/ul/li[2]/a').click()



def pt_Cuiaba():
    page = requests.get(config.url_pt_Cuiaba)
    soup = BeautifulSoup(page.content, 'html5lib')

    baixar_arquivo(soup, descricao='PLANILHA INFORMATIVA SOBRE OS GASTOS EMERGENCIAIS - SMASDH',
                   nome_arquivo='planilha_gastos_emergenciais_SMASDH.pdf')
    baixar_arquivo(soup, descricao='PLANILHA INFORMATIVA SOBRE OS GASTOS EMERGENCIAIS - SMS ',
                   nome_arquivo='planilha_gastos_emergenciais_SMS.pdf')
    baixar_arquivo(soup, descricao='PLANILHA INFORMATIVA SOBRE OS GASTOS EMERGENCIAIS - SMG - SME',
                   nome_arquivo='planilha_gastos_emergenciais_SMSU_SME_SMGE.pdf')
    baixar_arquivo(soup, descricao='RECURSOS RECEBIDOS E APLICADOS ', nome_arquivo='recursos_recebidos_aplicados.pdf')
    baixar_arquivo(soup, descricao='RELAÇÃO POR ITEM ', nome_arquivo='descritivos_aquisicoes.pdf')


def baixar_arquivo(soup, descricao, nome_arquivo):
    span = soup.find_all('span', string=descricao)[0]
    link = span.parent.find_all('a')[0].attrs['href']
    url = 'http://covid.cuiaba.mt.gov.br/' + link
    diretorio = os.path.join(config.diretorio_dados, 'MT', 'portal_transparencia', 'Cuiabá')
    download(url, diretorio, os.path.join(diretorio, nome_arquivo))


def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_MT = PortalTransparencia_MT()
    pt_MT.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Cuiaba()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
