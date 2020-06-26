import logging
import time
from os import path

from covidata import config
from covidata.webscraping.downloader import FileDownloader

def main():
    logger = logging.getLogger('covidata')
    logger.info('Tribunal de contas estadual...')
    start_time = time.time()
    tce = FileDownloader(path.join(config.diretorio_dados, 'RS', 'tce'), config.url_tce_RS,
                           'licitações_-_covid-19.xls')
    tce.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_RS = FileDownloader(path.join(config.diretorio_dados, 'RS', 'portal_transparencia'), config.url_pt_RS,
                         'editais-covid19.csv')
    pt_RS.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
