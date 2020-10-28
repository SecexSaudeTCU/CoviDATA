import logging
import time
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_AP_Scraper(Scraper):

    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        downloader = FileDownloader(path.join(config.diretorio_dados, 'AP', 'portal_transparencia'), config.url_pt_AP,
                                    'contratos.xlsx')
        downloader.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_contratacoes(data_extracao), False

    def consolidar_contratacoes(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'orgao',
                            consolidacao.DESPESA_DESCRICAO: 'objeto',
                            consolidacao.CONTRATADO_CNPJ: 'fornecedor_cnpj_cpf',
                            consolidacao.CONTRATADO_DESCRICAO: 'fornecedor_razao_social',
                            consolidacao.VALOR_CONTRATO: 'valor_total'}
        planilha_original = path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'contratos.xlsx')
        df_original = pd.read_excel(planilha_original)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AP
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'AP', '', data_extracao)
        df = df.astype({consolidacao.CONTRATADO_CNPJ: str})
        return df
