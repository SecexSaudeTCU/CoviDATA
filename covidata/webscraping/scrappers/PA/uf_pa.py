import json
import os
import time
from os import path

import pandas as pd

import config
from webscraping.downloader import FileDownloader


def pt_PA():
    downloader = FileDownloader(path.join(config.diretorio_dados, 'PA', 'portal_transparencia'), config.url_pt_PA,
                                'covid.json')
    downloader.download()

    with open(os.path.join(downloader.diretorio_dados, downloader.nome_arquivo)) as json_file:
        data = json.load(json_file)
        registros = data['Registros']

        colunas = []
        linhas = []

        for elemento in registros:
            linha = []

            for key, value in elemento['registro'].items():
                if not key in colunas:
                    colunas.append(key)
                linha.append(value)

            linhas.append(linha)

        df = pd.DataFrame(linhas, columns=colunas)
        df.to_excel(os.path.join(downloader.diretorio_dados, 'covid.xlsx'))


def main():
    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_PA()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Tribunal de Contas municipal...')
    start_time = time.time()
    tcm_PA_1 = FileDownloader(path.join(config.diretorio_dados, 'PA', 'tcm'), config.url_tcm_PA_1,
                              'Argus TCMPA - Fornecedores por Valor Homologado.xlsx')
    tcm_PA_1.download()

    tcm_PA_2 = FileDownloader(path.join(config.diretorio_dados, 'PA', 'tcm'), config.url_tcm_PA_2,
                              'Argus TCMPA - Fornecedores por Quantidade de Municípios.xlsx')
    tcm_PA_2.download()
    print("--- %s segundos ---" % (time.time() - start_time))


#main()
