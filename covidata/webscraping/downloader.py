import os
from os import path
from urllib import request

class FileDownloader:
    def __init__(self, diretorio_dados, url, nome_arquivo):
        self.diretorio_dados = diretorio_dados
        self.url = url
        self.nome_arquivo = nome_arquivo

    def download(self):
        if not path.exists(self.diretorio_dados):
            os.makedirs(self.diretorio_dados)

        request.urlretrieve(self.url, os.path.join(self.diretorio_dados, self.nome_arquivo))