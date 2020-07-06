import os
from os import path
import time
from glob import glob
import logging

import pandas as pd

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader


# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_RN(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte'),
                         config.url_pt_RN,
                         browser_option='--start-maximized')

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        # Seleciona o botão em forma de planilha do Excel
        self.driver.find_element_by_xpath('//*[@id="DataTables_Table_0_wrapper"]/div[1]/button[2]/span/i').click()

        # On hold por 3 segundos
        time.sleep(3)

        # Obtém objeto list dos arquivos armazenados no path passado como argumento para a função nativa "glob"
        list_files = glob(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte/*'))

        # Obtém o primeiro elemento do objeto list que corresponde ao nome do arquivo "csv" baixado
        file_name = list_files[0]

        # Lê o arquivo "csv" de nome "file_name" de contratos baixado como um objeto pandas DataFrame
        # Usa o parâmetro "error_bad_lines" como "False" para ignorar linhas com problema do arquivo "csv" (primeira solução)
        df_contratos = pd.read_csv(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte', file_name),
                                   sep=';', error_bad_lines=False)

        # Cria arquivo "xlsx" e aloca file handler de escrita para a variável "writer"
        with pd.ExcelWriter(path.join(config.diretorio_dados, 'RN', 'portal_transparencia',
                            'RioGrandeNorte', 'Dados_Portal_Transparencia_RioGrandeNorte.xlsx')) as writer:
            # Salva os dados de contratos contidos em "df_contratos" na planilha "Contratos"
            df_contratos.to_excel(writer, sheet_name='Contratos', index=False)

        # Deleta o arquivo de nome "file_name"
        os.unlink(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte', file_name))


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_RN = PortalTransparencia_RN()
    pt_RN.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
