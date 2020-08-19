import logging
import time
from os import path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar
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

        salvar(consolidacoes, 'MT')
        return consolidacoes, True

    def __consolidar_pt_MT(self, data_extracao):
        # Objeto dict em que os valores têm chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Entidade',
                            consolidacao.UG_DESCRICAO: 'Entidade',
                            consolidacao.NUMERO_PROCESSO: 'Númerodo Processo',
                            consolidacao.NUMERO_CONTRATO: 'Contrato',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.ITEM_EMPENHO_DESCRICAO: 'Item do Objeto',
                            consolidacao.ITEM_EMPENHO_UNIDADE_MEDIDA: 'Unidade',
                            consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Quantidade',
                            consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor Unitário',
                            consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'Valor Global',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.CONTRATADO_DESCRICAO: 'Razao Social',
                            consolidacao.DATA_CELEBRACAO: 'Data de Celebração',
                            consolidacao.DATA_VIGENCIA: 'Data de Vigência',
                            consolidacao.SITUACAO: 'Situação',
                            consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local da Execução'}

        # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
        colunas_adicionais = ['Modalidade de Contratação']

        df_original = pd.read_excel(path.join(config.diretorio_dados, 'MT', 'portal_transparencia', 'contratos.xls'),
                                    header=4)

        # Chama a função "pre_processar_tce" definida neste módulo
        #df = self.pre_processar_pt_MT(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MT,
                               'MT', '', data_extracao, self.pos_consolidar_pt_MT)

        return df

    def pre_processar_pt_MT(self, df):
        # Insere a substring "," na penúltima posição dos objetos string que compõem...
        # as colunas especificadas
        for col in np.array(['PU Item', 'Preço Item']):
            df[col] = df[col].apply(lambda x: x[:-2] + ',' + x[-2:])

        return df

    def pos_consolidar_pt_MT(self, df):
        try:
            df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
            df = df.astype({consolidacao.CONTRATADO_CNPJ: str})
        except ValueError:
            # Há linhas com dados inválidos para CNPJ (ex.: data)
            df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

        for i in range(0, len(df)):
            tamanho = len(df.loc[i, consolidacao.CONTRATADO_CNPJ])

            if tamanho > 2:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

                if tamanho < 14:
                    df.loc[i, consolidacao.CONTRATADO_CNPJ] = '0' * (14 - tamanho) + df.loc[
                        i, consolidacao.CONTRATADO_CNPJ]

            termos = df.loc[i, consolidacao.DATA_VIGENCIA].split()
            df.loc[i, consolidacao.DATA_INICIO_VIGENCIA] = termos[0]
            df.loc[i, consolidacao.DATA_FIM_VIGENCIA] = termos[2]

        df = df.drop([consolidacao.DATA_VIGENCIA], axis=1)

        return df
