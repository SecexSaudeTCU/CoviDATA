from datetime import datetime
import logging
import time

from covidata.webscraping.scrappers.SP import pt_sp, pt_sp_capital, tcm_sp, consolidacao_SP

def main():
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')

    pt_sp.main()
    pt_sp_capital.main()

    #TODO: Desabilitado devido a instabiliades do Selenium
    #tcm_sp.main()

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidacao_SP.consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))