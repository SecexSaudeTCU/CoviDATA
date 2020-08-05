from abc import ABC, abstractmethod


class NER(ABC):

    @abstractmethod
    def extrair_entidades(self, df):
        pass



