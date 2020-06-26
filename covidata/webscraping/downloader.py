import os
from os import path
import requests
import logging


class FileDownloader:
    def __init__(self, diretorio_dados, url, nome_arquivo):
        self.diretorio_dados = diretorio_dados
        self.url = url
        self.nome_arquivo = nome_arquivo

    def download(self):
        if not path.exists(self.diretorio_dados):
            os.makedirs(self.diretorio_dados)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/50.0.2661.102 Safari/537.36'}
        r = requests.get(self.url, headers=headers, verify=False)
        with open(os.path.join(self.diretorio_dados, self.nome_arquivo), 'wb') as f:
            f.write(r.content)


def download(url, diretorio, caminho_completo):
    logger = logging.getLogger('covidata')
    if not path.exists(diretorio):
        os.makedirs(diretorio)

    r = requests.get(url)
    with open(caminho_completo, 'wb') as f:
        logger.info(f'Fazendo o download do arquivo {caminho_completo}...')
        f.write(r.content)