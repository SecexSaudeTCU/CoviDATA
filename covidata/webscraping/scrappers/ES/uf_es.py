import logging
import time
from os import path

from covidata import config
from covidata.webscraping.downloader import FileDownloader


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_ES = FileDownloader(path.join(config.diretorio_dados, 'ES', 'portal_transparencia'), config.url_pt_ES,
                           'dados-contratos-emergenciais-covid-19.csv')
    pt_ES.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Vitoria = FileDownloader(path.join(config.diretorio_dados, 'ES', 'portal_transparencia', 'Vitoria'),
                               config.url_pt_Vitoria, 'TransparenciaWeb.Licitacoes.Lista.xlsx')
    pt_Vitoria.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
