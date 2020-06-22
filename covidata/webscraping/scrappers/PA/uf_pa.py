from webscraping.downloader import FileDownloader
from os import path
from webscraping.selenium.downloader import SeleniumDownloader
import time
import config
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class PortalTransparencia_Belem(SeleniumDownloader):

    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'PA', 'portal_transparencia', 'Belem'), url)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        frame = wait.until(EC.visibility_of_element_located((By.NAME, 'myiFrame')))
        self.driver.switch_to.frame(frame)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()


def main():
    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_PA = FileDownloader(path.join(config.diretorio_dados, 'PA', 'portal_transparencia'), config.url_pt_PA,
                           'covid.json')
    pt_PA.download()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência da capital...')
    start_time = time.time()
    pt_Belem = PortalTransparencia_Belem(config.url_pt_Belem)
    pt_Belem.download()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Tribunal de Contas municipal...')
    start_time = time.time()
    tcm_PA_1 = FileDownloader(path.join(config.diretorio_dados, 'PA', 'tcm'), config.url_tcm_PA_1,
                              'Argus TCMPA - Fornecedores por Valor Homologado.xlsx')
    tcm_PA_1.download()

    tcm_PA_2 = FileDownloader(path.join(config.diretorio_dados, 'PA', 'tcm'), config.url_tcm_PA_2,
                              'Argus TCMPA - Fornecedores por Quantidade de Municípios.xlsx')
    tcm_PA_2.download()
    print("--- %s segundos ---" % (time.time() - start_time))
