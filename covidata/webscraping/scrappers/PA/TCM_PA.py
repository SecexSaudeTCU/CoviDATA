from os import path

import logging
import os
import pandas as pd
import time

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import salvar
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class TCM_PA_Scraper1(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas municipal...')
        start_time = time.time()
        tcm_PA_1 = FileDownloader(path.join(config.diretorio_dados, 'PA', 'tcm'), config.url_tcm_PA_1,
                                  'Argus TCMPA - Fornecedores por Valor Homologado.xlsx')
        tcm_PA_1.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_tcm(data_extracao), True

    def __consolidar_tcm(self, data_extracao):
        # Salva arquivos adicionais (informações acessórias que podem ser relevantes)
        fornecedores_por_valor_homologado = pd.read_excel(os.path.join(config.diretorio_dados, 'PA', 'tcm',
                                                                       'Argus TCMPA - Fornecedores por Valor Homologado.xlsx'))
        fornecedores_por_valor_homologado[consolidacao.FONTE_DADOS] = config.url_tcm_PA_1
        fornecedores_por_valor_homologado[consolidacao.DATA_EXTRACAO_DADOS] = data_extracao
        salvar(fornecedores_por_valor_homologado, 'PA', '_fornecedores_por_valor_homologado')


class TCM_PA_Scraper2(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas municipal...')
        start_time = time.time()

        tcm_PA_2 = FileDownloader(path.join(config.diretorio_dados, 'PA', 'tcm'), config.url_tcm_PA_2,
                                  'Argus TCMPA - Fornecedores por Quantidade de Municípios.xlsx')
        tcm_PA_2.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_tcm(data_extracao), True

    def __consolidar_tcm(self, data_extracao):
        # Salva arquivos adicionais (informações acessórias que podem ser relevantes)
        fornecedores_por_qtd_municipios = pd.read_excel(os.path.join(config.diretorio_dados, 'PA', 'tcm',
                                                                     'Argus TCMPA - Fornecedores por Quantidade de Municípios.xlsx'))
        fornecedores_por_qtd_municipios[consolidacao.FONTE_DADOS] = config.url_tcm_PA_2
        fornecedores_por_qtd_municipios[consolidacao.DATA_EXTRACAO_DADOS] = data_extracao
        salvar(fornecedores_por_qtd_municipios, 'PA', '_fornecedores_por_qtd_municipios')
