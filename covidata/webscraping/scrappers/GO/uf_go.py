import datetime
import logging
import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.GO.consolidacao_GO import consolidar


class PT_Despesas_Goiania(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'GO', 'portal_transparencia', 'Goiania'),
                         config.url_pt_Goiania_despesas)

    def _executar(self):
        # Cria um objeto da class "WebDriverWait"
        wait = WebDriverWait(self.driver, 45)

        # Entra no iframe de id "modulo_portal"
        iframe = wait.until(EC.visibility_of_element_located((By.ID, 'modulo_portal')))
        self.driver.switch_to.frame(iframe)

        # Seleciona o menu dropdown "Exportar" e seta a opção "JSON"
        select = Select(self.driver.find_element_by_xpath(
            '//*[@id="WebPatterns_wt2_block_wtMainContent_DespesasWebBlocks_wt3_block_wtselExportar"]'))
        select.select_by_visible_text('JSON')


def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')

    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_GO = FileDownloader(path.join(config.diretorio_dados, 'GO', 'portal_transparencia'), config.url_pt_GO,
                           'aquisicoes.csv')
    pt_GO.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_despesas_Goiania = PT_Despesas_Goiania()
    pt_despesas_Goiania.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

#main()