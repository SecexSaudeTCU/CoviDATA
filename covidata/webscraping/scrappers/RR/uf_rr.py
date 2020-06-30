import logging
import time
from os import path

from covidata import config
from covidata.webscraping.downloader import FileDownloader


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_RR = FileDownloader(path.join(config.diretorio_dados, 'RR', 'portal_transparencia'), config.url_pt_RR,
                           'covid19_1592513934.xls')
    pt_RR.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_BoaVista = FileDownloader(path.join(config.diretorio_dados, 'RR', 'portal_transparencia', 'Boa Vista'),
                                 config.url_pt_BoaVista, 'contrato_covid-19.txt')
    pt_BoaVista.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
