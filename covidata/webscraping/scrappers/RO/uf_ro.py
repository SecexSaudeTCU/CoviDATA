import datetime
import logging
import time
from os import path

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.RO.consolidacao_RO import consolidar



# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_PortoVelho(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(str(config.diretorio_dados)[:-18], 'dados', 'RO', 'portal_transparencia', 'PortoVelho'),
                         config.url_pt_PortoVelho)

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        # Seleciona o elemento da lista "Despesas por Credor / Instituição"
        self.driver.find_element_by_xpath('//*[@id="consulta_dados"]/fieldset/div/ul/li[2]/a').click()

        # On hold por 5 segundos
        time.sleep(5)

        # Seleciona o botão que tem como símbolo um arquivo "csv"
        self.driver.find_element_by_xpath('//*[@id="main_content"]/div[3]/span[3]').click()



def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt = FileDownloader(path.join(str(config.diretorio_dados)[:-18], 'dados', 'RO', 'portal_transparencia'), config.url_pt_RO,
                        'Despesas.CSV')
    pt.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_PortoVelho = PortalTransparencia_PortoVelho()
    pt_PortoVelho.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    # logger.info('Consolidando as informações no layout padronizado...')
    # start_time = time.time()
    # consolidar(data_extracao)
    # logger.info("--- %s segundos ---" % (time.time() - start_time))
