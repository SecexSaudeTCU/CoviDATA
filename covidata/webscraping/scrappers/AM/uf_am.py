import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import config
from webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_AM(SeleniumDownloader):
    def __init__(self, diretorio_dados, url):
        super().__init__(path.join(diretorio_dados, 'AM', 'portal_transparencia'), url)

    def download(self):
        wait = WebDriverWait(self.driver, 30)

        frame = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, 'page-iframe')))
        self.driver.switch_to.frame(frame)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)

        # Aguarda o download
        time.sleep(5)

        self.driver.switch_to.default_content()
        self.driver.close()
        self.driver.quit()


# Em tese, esta carga não precisaria ser via Selenium, porém tem a vantagem de abstrair a URL direta do arquivo, que no
# momento contém, por exemplo, "2020/06".
class PortalTransparencia_Manaus(SeleniumDownloader):
    def __init__(self, diretorio_dados, url):
        super().__init__(path.join(diretorio_dados, 'AM', 'portal_transparencia', 'Manaus'), url)

    def download(self):
        button = self.driver.find_element_by_id('btn_csv')
        button.click()

        # Aguarda o download
        time.sleep(5)

        self.driver.close()
        self.driver.quit()


def main():
    pt_AM = PortalTransparencia_AM(config.diretorio_dados, config.url_pt_AM)
    pt_Manaus = PortalTransparencia_Manaus(config.diretorio_dados, config.url_pt_Manaus)

    pt_AM.download()
    pt_Manaus.download()
