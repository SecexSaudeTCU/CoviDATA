import logging
import os
import time
from os import path

import numpy as np
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_municipios_por_uf
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class TCE_MA_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de contas estadual...')
        start_time = time.time()
        tce_ma = TCE_MA_Downloader()
        tce_ma.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

        # Renomeia o arquivo
        diretorio = path.join(config.diretorio_dados, 'MA', 'tce')
        arquivo = os.listdir(diretorio)[0]
        os.rename(path.join(diretorio, arquivo), path.join(diretorio, 'licitacoes.xls'))

    def consolidar(self, data_extracao):
        return self.__consolidar_licitacoes(data_extracao), False

    def __consolidar_licitacoes(self, data_extracao):
        dicionario_dados = {consolidacao.MUNICIPIO_DESCRICAO: 'ENTE', consolidacao.CONTRATANTE_DESCRICAO: 'UNIDADE',
                            consolidacao.UG_DESCRICAO: 'UNIDADE', consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                            consolidacao.VALOR_CONTRATO: 'VALOR',
                            consolidacao.FONTE_RECURSOS_DESCRICAO: 'ORIGEM DO RECURSO',
                            consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO',
                            consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ',
                            consolidacao.DATA_ASSINATURA: 'DATA ASSINATURA',
                            consolidacao.NUMERO_CONTRATO: 'Nº CONTRATO',
                            consolidacao.NUMERO_PROCESSO: 'Nº PROCESSO'}
        colunas_adicionais = ['VIGÊNCIA']
        planilha_original = path.join(config.diretorio_dados, 'MA', 'tce', 'licitacoes.xls')
        df_original = pd.read_excel(planilha_original, header=1)
        fonte_dados = consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_MA
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'MA', '', data_extracao, self.pos_processar_licitacoes)
        return df

    def pos_processar_licitacoes(self, df):
        df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
        df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

        codigos_municipios = get_municipios_por_uf('MA')
        # Define os municípios
        df[consolidacao.COD_IBGE_MUNICIPIO] = df.apply(
            lambda row: codigos_municipios.get(row[consolidacao.MUNICIPIO_DESCRICAO].upper(), ''), axis=1)
        df = df.astype({consolidacao.COD_IBGE_MUNICIPIO: str})

        df.loc[(df[consolidacao.COD_IBGE_MUNICIPIO] != ''), consolidacao.ESFERA] = consolidacao.ESFERA_MUNICIPAL

        for i in range(0, len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(cpf_cnpj) == 11:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
            elif len(cpf_cnpj) > 11:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

            vigencia = df.loc[i, 'VIGÊNCIA']
            data_inicio = vigencia[0:vigencia.find(' à ')]
            data_fim = vigencia[vigencia.find(' à ') + 3:len(vigencia)]
            df.loc[i, consolidacao.DATA_INICIO_VIGENCIA] = data_inicio
            df.loc[i, consolidacao.DATA_FIM_VIGENCIA] = data_fim

        df = df.drop(['VIGÊNCIA'], axis=1)
        return df


class TCE_MA_Downloader(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MA', 'tce'), config.url_tce_MA)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        element = wait.until(EC.element_to_be_clickable((By.ID, 'z_n')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()