import datetime
from covidata import config
import logging
import time
from os import path
from covidata.webscraping.downloader import FileDownloader
import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader

class PortalTransparencia_Curitiba(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                         config.url_pt_Curitiba_aquisicoes)

    def _executar(self):
        button = self.driver.find_element_by_class_name('excel')
        button.click()

def portal_transparencia_PR():
    agora = datetime.datetime.now()
    mes_atual = agora.month

    if mes_atual <= 9:
        mes_atual = '0' + str(mes_atual)

    ano_atual = agora.year

    nome_arquivo_aquisicoes = 'aquisicoes_e_contratacoes_0.xls'
    url = f'{config.url_pt_PR}{ano_atual}-{mes_atual}/{nome_arquivo_aquisicoes}'

    pt_PR_aquisicoes = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia'), url,
                                      nome_arquivo_aquisicoes)
    pt_PR_aquisicoes.download()

    nome_arquivo_dados_abertos = 'dados_abertos.xlsx'
    url = f'{config.url_pt_PR}{ano_atual}-{mes_atual}/{nome_arquivo_dados_abertos}'

    pt_PR_dados_abertos = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia'), url,
                                         nome_arquivo_dados_abertos)
    pt_PR_dados_abertos.download()


def portal_transparencia_Curitiba():
    pt_contratacoes = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                                 config.url_pt_Curitiba_contratacoes, 'licitacoes_contratacoes.csv')
    pt_contratacoes.download()

    pt_aquisicoes = PortalTransparencia_Curitiba()
    pt_aquisicoes.download()

def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()

    portal_transparencia_PR()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()

    portal_transparencia_Curitiba()

    logger.info("--- %s segundos ---" % (time.time() - start_time))


#main()
