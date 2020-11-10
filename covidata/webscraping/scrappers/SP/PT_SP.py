from os import path

import logging
import pandas as pd
import time

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_SP_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        downloader = FileDownloader(path.join(config.diretorio_dados, 'SP', 'portal_transparencia'), self.url,
                                    'COVID.csv')
        downloader.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_PT(data_extracao), False

    def consolidar_PT(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Secretaria',
                            consolidacao.CONTRATADO_DESCRICAO: 'Contratada / Conveniada',
                            consolidacao.CONTRATADO_CNPJ: 'CPF / CNPJ / CGC',
                            consolidacao.DESPESA_DESCRICAO: 'Descrição Processo',
                            consolidacao.DOCUMENTO_NUMERO: 'Nota de Empenho',
                            consolidacao.DOCUMENTO_DATA: 'Data da Movimentação'}
        df_original = pd.read_csv(path.join(config.diretorio_dados, 'SP', 'portal_transparencia', 'COVID.csv'),
                                  encoding='ISO-8859-1', sep=';')
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SP, 'SP',
                               None, data_extracao)
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
        return df


