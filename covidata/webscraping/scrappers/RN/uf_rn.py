import logging
import time
from datetime import datetime
from os import path

from covidata import config
from covidata.webscraping.scrappers.RN.consolidacao_RN import consolidar
from covidata.webscraping.selenium.downloader import SeleniumDownloader


# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_Natal(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'Natal'),
                         config.url_pt_Natal)

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        # On hold por 3 segundos
        time.sleep(3)

        # Seleciona o botão em forma de planilha do Excel
        self.driver.find_element_by_xpath('//*[@id="DataTables_Table_0_wrapper"]/div[1]/button[2]/span').click()



def main(df_consolidado):
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Natal = PortalTransparencia_Natal()
    pt_Natal.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

#main()