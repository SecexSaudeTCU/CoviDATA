import time
import pandas as pd
import logging

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper
from os import path

class TCE_RS_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de contas estadual...')
        start_time = time.time()
        tce = FileDownloader(path.join(config.diretorio_dados, 'RS', 'tce'), config.url_tce_RS,
                             'licitações_-_covid-19.xls')
        tce.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_tce(data_extracao), False

    def consolidar_tce(self, data_extracao):
        # Objeto dict em que os valores têm chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto'}

        # Lê o arquivo "xls" de licitações baixado como um objeto list utilizando a função "read_html" da biblioteca pandas
        df_original = pd.read_html(path.join(config.diretorio_dados, 'RS', 'tce', 'licitações_-_covid-19.xls'),
                                   decimal=',')

        # Chama a função "pre_processar_tce" definida neste módulo
        df = self.pre_processar_tce(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_RS, 'RS', '', data_extracao)

        return df

    def pre_processar_tce(self, df):
        # Lê o primeiro elemento do objeto list que se constitui em um objeto pandas DataFrame
        df = df[0]

        # Elimina a primeira linha do objeto pandas DataFrame "df"
        df.drop(0, axis=0, inplace=True)

        # Reseta o index de "df"
        df.reset_index(drop=True, inplace=True)

        # Renomeia as colunas especificadas
        df.rename(index=str,
                  columns={0: 'Órgão',
                           1: 'Modalidade Licitação',
                           2: 'Número Licitação',
                           3: 'Ano',
                           4: 'Número Processo',
                           5: 'Objeto',
                           6: 'Valor Homologado',
                           7: 'Resultado Licitação',
                           8: 'Vencedor Licitação'},
                  inplace=True)

        return df