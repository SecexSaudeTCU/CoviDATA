import datetime
import logging
import time

from covidata.webscraping.scrappers.RJ import tce_rj, pt_rj_capital, consolidacao_RJ

def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')

    tce_rj.main()
    pt_rj_capital.main()

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidacao_RJ.consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
