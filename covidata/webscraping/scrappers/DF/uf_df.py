import os
from os import path
import time
import logging

import pandas as pd

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader


# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_DF(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'DF', 'portal_transparencia',
                         'DistritoFederal'),
                         config.url_pt_DF)

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        # Seleciona o botão "Dados abertos"
        self.driver.find_element_by_xpath('//*[@id="conteudo"]/div[3]/div/div[1]/a').click()

        # On hold por 3 segundos
        time.sleep(5)

        # Lê o arquivo "csv" de nome "PLANILHA-COVID" de contratos baixado como um objeto pandas DataFrame
        df_contratos = pd.read_csv(path.join(config.diretorio_dados, 'DF', 'portal_transparencia',
                                   'DistritoFederal', 'PLANILHA-COVID.csv'),
                                   sep=';')

        # Cria arquivo "xlsx" e aloca file handler de escrita para a variável "writer"
        with pd.ExcelWriter(path.join(config.diretorio_dados, 'DF', 'portal_transparencia',
                            'DistritoFederal', 'Dados_Portal_Transparencia_DistritoFederal.xlsx')) as writer:
            # Salva os dados de contratos contidos em "df_contratos" na planilha "Contratos"
            df_contratos.to_excel(writer, sheet_name='Contratos', index=False)

        # Deleta o arquivo "csv" de nome "PLANILHA-COVID"
        os.unlink(path.join(config.diretorio_dados, 'DF', 'portal_transparencia', 'DistritoFederal',
                  'PLANILHA-COVID.csv'))


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência distrital...')
    start_time = time.time()
    pt_DF = PortalTransparencia_DF()
    pt_DF.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
