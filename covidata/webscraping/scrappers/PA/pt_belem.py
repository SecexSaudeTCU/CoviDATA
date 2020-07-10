import logging
import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_Belem(SeleniumDownloader):

    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'PA', 'portal_transparencia', 'Belem'), url)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        #Aguarda pelo carregamento completo da página
        time.sleep(10)

        frame = wait.until(EC.visibility_of_element_located((By.NAME, 'myiFrame')))
        self.driver.switch_to.frame(frame)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()

def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Belem = PortalTransparencia_Belem(config.url_pt_Belem)
    pt_Belem.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
