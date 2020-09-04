import logging
import time
from os import path

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_CampoGrande_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        start_time = time.time()
        pt_CampoGrande = PortalTransparencia_CampoGrande()
        pt_CampoGrande.download()

        logger.info("--- %s segundos ---" % (time.time() - start_time))


    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Mato Grosso do Sul')

        despesas_capital = self.__consolidar_despesas_capital(data_extracao)
        return despesas_capital, False

    def __consolidar_despesas_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.UG_DESCRICAO: 'Unidade',
                            consolidacao.CONTRATADO_DESCRICAO: 'Fornecedor',
                            consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'Elemento Despesa',
                            consolidacao.VALOR_EMPENHADO: 'Total Empenhado',
                            consolidacao.VALOR_LIQUIDADO: 'Total Liquidado', consolidacao.VALOR_PAGO: 'Total Pago',
                            consolidacao.CATEGORIA_ECONOMICA_DESCRICAO: 'Categoria'}
        colunas_adicionais = ['Processo de Origem', 'Data', 'Status']
        planilha_original = path.join(config.diretorio_dados, 'MS', 'portal_transparencia', 'Campo Grande',
                                      'Despesas – Transparência Covid19 – Prefeitura de Campo Grande.xlsx')
        df_original = pd.read_excel(planilha_original, header=1)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_CampoGrande
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'MS', get_codigo_municipio_por_nome('Campo Grande', 'MS'), data_extracao,
                               self.pos_processar_despesas_capital)
        return df

    def pos_processar_despesas_capital(self, df):
        df = df.rename(columns={'PROCESSO DE ORIGEM': 'NÚMERO PROCESSO'})
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Campo Grande'
        return df

class PortalTransparencia_CampoGrande(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MS', 'portal_transparencia', 'Campo Grande'),
                         config.url_pt_CampoGrande)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        # Aguarda o carregamento completo da página.
        time.sleep(5)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)