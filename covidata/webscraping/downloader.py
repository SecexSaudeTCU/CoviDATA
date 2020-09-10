import os
from os import path
import requests
import logging


class FileDownloader:
    """
    Classe utilitária responsável pelo download de arquivos.
    """

    def __init__(self, diretorio_dados, url, nome_arquivo):
        """
        Construtor da classe.
        :param diretorio_dados: Diretório onde o arquivo deverá ser salvo.
        :param url: URL para o arquivo.
        :param nome_arquivo: Nome com o qual o arquivo deverá ser salvo.
        """
        self.diretorio_dados = diretorio_dados
        self.url = url
        self.nome_arquivo = nome_arquivo

    def download(self):
        """
        Executa o download do arquivo.
        :return: O HTTP status code da requisição.
        """
        if not path.exists(self.diretorio_dados):
            os.makedirs(self.diretorio_dados)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/50.0.2661.102 Safari/537.36'}
        r = requests.get(self.url, headers=headers, verify=False, timeout=60)

        if r.status_code != 500:
            caminho_arquivo = os.path.join(self.diretorio_dados, self.nome_arquivo)

            with open(caminho_arquivo, 'wb') as f:
                f.write(r.content)

        return r.status_code


def download(url, diretorio, caminho_completo):
    """
    Executa o download de um arquivo.
    :param url: URL para o arquivo.
    :param diretorio: Diretório onde o arquivo deverá ser salvo.
    :param caminho_completo: Caminho absoluto onde o arquivo deverá ser salvo, incluindo o nome do arquivo.
    :return:
    """
    logger = logging.getLogger('covidata')
    if not path.exists(diretorio):
        os.makedirs(diretorio)

    r = requests.get(url)
    with open(caminho_completo, 'wb') as f:
        logger.info(f'Fazendo o download do arquivo {caminho_completo}...')
        f.write(r.content)
