import os
from os import path
import requests

class FileDownloader:
    def __init__(self, diretorio_dados, url, nome_arquivo):
        self.diretorio_dados = diretorio_dados
        self.url = url
        self.nome_arquivo = nome_arquivo

    def download(self):
        if not path.exists(self.diretorio_dados):
            os.makedirs(self.diretorio_dados)

        r = requests.get(self.url)
        with open(os.path.join(self.diretorio_dados, self.nome_arquivo), 'wb') as f:
            f.write(r.content)