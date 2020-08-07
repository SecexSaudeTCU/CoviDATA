import locale

import os
from abc import ABC, abstractmethod
from os import path

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time
from sys import platform


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

    def download(self):
        """
        Executa o download, por meio da chamada ao método "executar()", a ser implementado pela subclasse, e posterior
        liberação dos objetos Selenium Web Driver associados..
        :return:
        """
        self._executar()

        # Aguarda o download
        time.sleep(5)

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
        else:
            locale.setlocale(locale.LC_ALL, "pt_br")

        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)

        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow', 'downloadPath': diretorio_dados}}
        command_result = driver.execute("send_command", params)

        return driver
