import datetime
import logging
import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.util.excel import exportar_arquivo_para_xlsx
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.PR.consolidacao_PR import consolidar
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_Curitiba(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                         config.url_pt_Curitiba_aquisicoes)

    def _executar(self):
        # Seleciona o campo "Data Início" e seta a data de início de busca
        campo_data_inicial = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.NAME, "ctl00$cphMasterPrincipal$txtDataInicial")))
        campo_data_inicial.send_keys(Keys.HOME)
        campo_data_inicial.send_keys('01032020')

        button = self.driver.find_element_by_class_name('excel')
        button.click()

        # Aqui, não é possível confiar na checagem de download da superclasse, uma vez que no mesmo diretório há outros
        # arquivos.
        time.sleep(5)


def portal_transparencia_Curitiba():
    pt_contratacoes = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                                     config.url_pt_Curitiba_contratacoes, 'licitacoes_contratacoes.csv')
    pt_contratacoes.download()

    pt_aquisicoes = PortalTransparencia_Curitiba()
    pt_aquisicoes.download()


def main(df_consolidado):
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência da capital...')
    start_time = time.time()

    portal_transparencia_Curitiba()

    exportar_arquivo_para_xlsx(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                               'Aquisições_para_enfrentamento_da_pandemia_do_COVID-19_-_Transparência_Curitiba.xls',
                               'aquisicoes.xlsx')

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao,df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))


