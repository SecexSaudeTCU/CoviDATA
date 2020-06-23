import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_AM(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'AM', 'portal_transparencia'), config.url_pt_AM)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        frame = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, 'page-iframe')))
        self.driver.switch_to.frame(frame)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()


# Em tese, esta carga não precisaria ser via Selenium, porém tem a vantagem de abstrair a URL direta do arquivo, que no
# momento contém, por exemplo, "2020/06".
class PortalTransparencia_Manaus(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'Manaus'),
                         config.url_pt_Manaus)

    def _executar(self):
        button = self.driver.find_element_by_id('btn_csv')
        button.click()


def main():
    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_AM = PortalTransparencia_AM()
    pt_AM.download()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência da capital...')
    start_time = time.time()
    pt_Manaus = PortalTransparencia_Manaus()
    pt_Manaus.download()
    print("--- %s segundos ---" % (time.time() - start_time))
