from abc import ABC, abstractmethod

class Scraper(ABC):
    def __init__(self, url):
        self.url = url

    @abstractmethod
    def scrap(self):
        pass

    @abstractmethod
    def consolidar(self, data_extracao):
        pass