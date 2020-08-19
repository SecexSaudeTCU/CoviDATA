from abc import ABC, abstractmethod


class Scraper(ABC):
    def __init__(self, url):
        self.url = url

    @abstractmethod
    def scrap(self):
        pass

    @abstractmethod
    def consolidar(self, data_extracao):
        """
        Consolida os dados em um dataframe final.  Retorna uma tupla (dataframe, bool), onde o segundo elemento informa
        se o dataframe foi salvo ou não.
        """
        pass
