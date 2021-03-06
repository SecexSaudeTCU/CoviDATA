from os import path

import logging
import numpy as np
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_MT_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()

        page = requests.get(config.url_pt_MT)
        soup = BeautifulSoup(page.content, 'html.parser')
        tabela = soup.find_all('table')[0]
        ths = tabela.find_all('th')
        colunas = [th.get_text() for th in ths]
        trs = tabela.find_all('tr')
        linhas = []

        for tr in trs:
            tds = tr.find_all('td')
            if len(tds) > 0:
                linhas.append([td.get_text().strip() for td in tds])

        df = pd.DataFrame(data=linhas, columns=colunas)
        persistir(df, 'portal_transparencia', 'contratos', 'MT')

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Mato Grosso')

        consolidacoes = self.__consolidar_pt_MT(data_extracao)

        return consolidacoes, False

    def __consolidar_pt_MT(self, data_extracao):
        # Objeto dict em que os valores têm chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Entidade',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.CONTRATADO_DESCRICAO: 'Razão Social',
                            consolidacao.VALOR_CONTRATO: 'Valor Global'}

        df_original = pd.read_excel(path.join(config.diretorio_dados, 'MT', 'portal_transparencia', 'contratos.xls'),
                                    header=4)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MT,
                               'MT', '', data_extracao, self.pos_processar)
        return df

    def pos_processar(self, df):
        # Remove a notação científica
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(np.int64)
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(str)

        return df
