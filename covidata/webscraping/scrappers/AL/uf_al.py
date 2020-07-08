import datetime
import logging
import time
from os import path

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.json.parser import JSONParser
from covidata.webscraping.scrappers.AL.consolidacao_AL import consolidar


class PortalTransparencia_Maceio(JSONParser):

    def __init__(self):
        super().__init__(config.url_pt_Maceio, 'num_processo', 'compras', 'portal_transparencia', 'AL', 'Maceio')

    def _get_elemento_raiz(self, conteudo):
        return conteudo['data']


def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_AL = FileDownloader(path.join(config.diretorio_dados, 'AL', 'portal_transparencia'), config.url_pt_AL,
                           'DESPESAS COM COVID-19.xls')
    pt_AL.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Maceio = PortalTransparencia_Maceio()
    pt_Maceio.parse()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
