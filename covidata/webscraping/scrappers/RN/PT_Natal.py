import logging

import time
from os import path
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_Natal_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Natal = PortalTransparencia_Natal()
        pt_Natal.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_Natal(data_extracao), False

    def consolidar_pt_Natal(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                            consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ'}

        # Lê o arquivo "xlsx" de nome de despesas baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'Natal', 'Natal Transparente.xlsx'),
            skiprows=[0])

        # Chama a função "pre_processar_pt_Natal" definida neste módulo
        df = self.pre_processar_pt_Natal(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Natal, 'RN',
                               get_codigo_municipio_por_nome('Natal', 'RN'), data_extracao)
        return df

    def pre_processar_pt_Natal(self, df):
        # Reordena as colunas do objeto pandas DataFrame "df"
        df = df[['Credor', 'CPF/CNPJ', 'Empenhado', 'Anulado', 'Liquidado', 'Pago']]

        # Preenche com zeros à esquerda a coluna especificada convertida em string até ter...
        # tamanho 14
        df['CPF/CNPJ'] = df['CPF/CNPJ'].apply(lambda x: str(x).zfill(14))

        return df


# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_Natal(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'Natal'),
                         config.url_pt_Natal)

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        # On hold por 3 segundos
        time.sleep(3)

        # Seleciona o botão em forma de planilha do Excel
        #/html/body/app-root/app-despesas-covid-credor/section[3]/div/div/div/div/div[2]/div/div[1]/button[2]
        #//*[@id="DataTables_Table_0_wrapper"]/div[1]/button[2]

        #self.driver.find_element_by_xpath('//*[@id="DataTables_Table_0_wrapper"]/div[1]/button[2]/span').click()
        self.driver.find_element_by_xpath('//*[@id="DataTables_Table_0_wrapper"]/div[1]/button[2]').click()