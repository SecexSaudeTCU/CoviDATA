import datetime
import logging
import os
import time
from os import path

from covidata import config
from covidata.webscraping.scrappers.AP.PT_AP import PortalTransparencia_AP
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.scrappers.AP.consolidacao_AP import consolidar


class PortalTransparencia_Macapa(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa'),
                         config.url_pt_Macapa)

    def _executar(self):
        button = self.driver.find_element_by_class_name('buttons-excel')
        button.click()


def main(df_consolidado):
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')

    """
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_AP = PortalTransparencia_AP()
    pt_AP.parse()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
    """

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Macapa = PortalTransparencia_Macapa()
    pt_Macapa.download()

    # Renomeia o arquivo
    diretorio = path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa')
    arquivo = os.listdir(diretorio)[0]
    os.rename(path.join(diretorio, arquivo), path.join(diretorio, 'transparencia.xlsx'))

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

#main()