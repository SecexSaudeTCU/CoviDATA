import logging
import time
from os import path

from covidata import config
from covidata.webscraping.json.parser import JSONParser
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.scrappers.AP.consolidacao_AP import consolidar

class PortalTransparencia_AP(JSONParser):

    def __init__(self):
        super().__init__(config.url_pt_AP, 'id', 'contratacoes', 'portal_transparencia', 'AP')

    def _get_elemento_raiz(self, conteudo):
        # Neste caso, não há elemento-raiz nomeado.
        return conteudo


class PortalTransparencia_Macapa(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa'), url)

    def _executar(self):
        button = self.driver.find_element_by_class_name('buttons-excel')
        button.click()


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_AP = PortalTransparencia_AP()
    pt_AP.parse()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Macapa = PortalTransparencia_Macapa(config.url_pt_Macapa)
    pt_Macapa.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

