import datetime
import logging
import os
import time
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_DF_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência distrital...')
        start_time = time.time()
        agora = datetime.datetime.now()
        mes_atual = str(agora.month)

        if int(mes_atual) < 10:
            mes_atual = '0' + mes_atual

        FileDownloader(os.path.join(config.diretorio_dados, 'DF', 'portal_transparencia'),
                       config.url_pt_DF + mes_atual + '/PLANILHA-COVID2.csv',
                       'PLANILHA-COVID2.csv').download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        start_time = time.time()
        logger.info('Iniciando consolidação dados Distrito Federal')
        dados = self.__consolidar_dados_portal_transparencia(data_extracao)
        salvar(dados, 'DF')
        logger.info("--- %s segundos ---" % (time.time() - start_time))
        return dados, True

    def __consolidar_dados_portal_transparencia(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO',
                            consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                            consolidacao.VALOR_CONTRATO: 'VALOR TOTAL'}

        planilha_original = path.join(config.diretorio_dados, 'DF', 'portal_transparencia', 'PLANILHA-COVID2.csv')
        df_original = pd.read_csv(planilha_original, sep=';')
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_DF
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'DF', '', data_extracao)
        return df
