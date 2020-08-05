import logging
import os
import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_SP(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'SP', 'portal_transparencia'), url)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        frame = wait.until(EC.visibility_of_element_located((By.TAG_NAME, 'iframe')))
        src = frame.get_attribute("src")

        self.driver.get(src)

        element = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, '[CSV]')))
        self.driver.execute_script("arguments[0].click();", element)


def main():
    logger = logging.getLogger('covidata')
    # TODO: Indisponivel
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_SP = PortalTransparencia_SP(config.url_pt_SP)
    pt_SP.download()

    diretorio = path.join(config.diretorio_dados, 'SP', 'portal_transparencia')
    arquivo = os.listdir(diretorio)[0]

    if '.csv.crdownload' in arquivo:
        os.rename(path.join(diretorio, arquivo), path.join(diretorio, 'COVID.csv'))

    logger.info("--- %s segundos ---" % (time.time() - start_time))

#main()