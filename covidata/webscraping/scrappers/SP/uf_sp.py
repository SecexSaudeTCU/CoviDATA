from datetime import datetime
import logging
import time

from covidata.webscraping.scrappers.SP import tcm_sp, consolidacao_SP


def main(df_consolidado):
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')

    # TODO: Desabilitado devido a instabiliades do Selenium
    tcm_sp.main()

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidacao_SP.consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

# main()
