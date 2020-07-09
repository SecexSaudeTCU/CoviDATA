import datetime
import logging
import time
from os import path

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.ES.consolidacao_ES import consolidar


def main():
    data_extracao = datetime.datetime.now()
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

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
