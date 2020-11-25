from os import path

import locale
import logging
import os
import time
from abc import ABC, abstractmethod
from selenium import webdriver
from sys import platform
from webdriver_manager.chrome import ChromeDriverManager


class SeleniumDownloader(ABC):
    """
    Classe utilitária abstrata responsável pelo download de arquivos por meio da execução de scripts Selenium Web
    Driver.
    """

    def __init__(self, diretorio_dados, url, browser_option='--headless'):
        """
        Construtor da classe
        :param diretorio_dados: Diretório onde os dados deverão ser salvos.
        :param url: URL da página do qual será executado o download.
        """

        self.driver = self.__configurar_chrome(diretorio_dados, browser_option)
        self.driver.get(url)
        self.diretorio_dados = diretorio_dados

    def download(self):
        """
        Executa o download, por meio da chamada ao método "executar()", a ser implementado pela subclasse, e posterior
        liberação dos objetos Selenium Web Driver associados..
        :return:
        """
        self._executar()

        # Aguarda o download
        self.aguardar_download()

        self.driver.close()
        self.driver.quit()

    @abstractmethod
    def _executar(self):
        """
        Método que deverá conter a lógica necessária ao download (ex.: localização do link ou botão de download a ser
        acionado e respectivo acionamento (ex.: execução de evento de clique).
        :return:
        """
        pass

    def __configurar_chrome(self, diretorio_dados, browser_option):
        if not path.exists(diretorio_dados):
            os.makedirs(diretorio_dados)

        chromeOptions = webdriver.ChromeOptions()
        prefs = {"download.default_directory": diretorio_dados}
        chromeOptions.add_experimental_option("prefs", prefs)
        chromeOptions.add_argument(browser_option)

        chromeOptions.add_argument("--no-sandbox")
        chromeOptions.add_argument("--disable-dev-shm-usage");

        if platform == "linux" or platform == "linux2":
            locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

            # TODO Parametrizar estes caminhos
            chromeOptions.binary_location = '/home/moniquebm/centos/usr/bin/chromium-browser'

            # options = webdriver.ChromeOptions()
            # options.gpu = False
            # options.headless = True
            # options.add_experimental_option("prefs", {
            #     "download.default_directory": diretorio_dados,
            #     'profile.default_content_setting_values.automatic_downloads': 2,
            # })
            #
            # desired = options.to_capabilities()
            # desired['loggingPrefs'] = {'performance': 'ALL'}
            # driver = webdriver.Chrome('/home/moniquebm/centos/usr/bin/chromedriver', chrome_options=chromeOptions,
            #                           desired_capabilities=desired)

            chrome_options = webdriver.ChromeOptions()
            preferences = {"download.default_directory": diretorio_dados,
                           "directory_upgrade": True,
                           "safebrowsing.enabled": True}
            chrome_options.add_experimental_option("prefs", preferences)
            driver = webdriver.Chrome(chrome_options=chrome_options,
                                      executable_path='/home/moniquebm/centos/usr/bin/chromedriver')
        else:
            locale.setlocale(locale.LC_ALL, "pt_br")
            driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
            driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
            params = {'cmd': 'Page.setDownloadBehavior',
                      'params': {'behavior': 'allow', 'downloadPath': diretorio_dados}}
            driver.execute("send_command", params)

        return driver

    def aguardar_download(self):
        logger = logging.getLogger('covidata')
        i = 1

        while len(os.listdir(self.diretorio_dados)) == 0 and i < 30:
            # Aguarda o download do arquivo
            logger.info(f"Aguardando download do arquivo... (tentativa {i})")
            time.sleep(5)
            i += 1

        if i < 30:
            logger.info("Download concluído.")
