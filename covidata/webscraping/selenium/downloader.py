import os
from abc import ABC, abstractmethod
from os import path

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager


class SeleniumDownloader(ABC):
    def __init__(self, diretorio_dados, url):
        self.driver = self.__configurar_chrome(diretorio_dados)
        self.driver.get(url)

    @abstractmethod
    def download(self):
        pass

    def __configurar_chrome(self, diretorio_dados):
        if not path.exists(diretorio_dados):
            os.makedirs(diretorio_dados)

        chromeOptions = webdriver.ChromeOptions()
        prefs = {"download.default_directory": diretorio_dados}
        chromeOptions.add_experimental_option("prefs", prefs)
        chromeOptions.add_argument('--headless')

        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)

        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow', 'downloadPath': diretorio_dados}}
        command_result = driver.execute("send_command", params)

        return driver