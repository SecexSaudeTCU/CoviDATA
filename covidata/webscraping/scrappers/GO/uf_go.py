import logging
import time
from os import path

from covidata import config
from covidata.webscraping.downloader import FileDownloader


def pt_GO():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_GO = FileDownloader(path.join(config.diretorio_dados, 'GO', 'portal_transparencia'), config.url_pt_GO,
                           'aquisicoes.csv')
    pt_GO.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))


def main():
    pt_GO()


main()
