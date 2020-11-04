import os

import json
import logging
import time
from os import path

import pandas as pd
import requests

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar
from covidata.webscraping.json.parser import JSONParser
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_AL_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        resultado = json.loads(requests.get(config.url_pt_AL).content)
        total = resultado['total']

        url = config.url_pt_AL + f'&limit={total}'
        json_parser = PortalTransparenciaAlagoas(url)
        json_parser.parse()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_despesas(data_extracao)

    def __consolidar_despesas(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'cpf_cnpj_contratado',
                            consolidacao.DESPESA_DESCRICAO: 'objeto',
                            consolidacao.CONTRATANTE_DESCRICAO: 'orgao_contratante',
                            consolidacao.CONTRATADO_DESCRICAO: 'nome_contratado',
                            consolidacao.DOCUMENTO_NUMERO: 'nota_empenho',
                            consolidacao.VALOR_CONTRATO: 'valor_total'}
        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'AL', 'portal_transparencia', 'despesas.xlsx'))
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AL, 'AL', '',
                               data_extracao)
        df[consolidacao.TIPO_DOCUMENTO] = 'EMPENHO'
        return df, False


class PortalTransparenciaAlagoas(JSONParser):
    def __init__(self, url):
        super(PortalTransparenciaAlagoas, self).__init__(url, 'nota_empenho', 'despesas', 'portal_transparencia', 'AL')

    def _get_elemento_raiz(self, conteudo):
        return conteudo['rows']
