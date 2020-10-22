from os import path

import logging
import os
import pandas as pd
import time

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar
from covidata.webscraping.json.parser import JSONParser
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_Maceio_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Maceio = PortalTransparencia_Maceio()
        pt_Maceio.parse()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_compras_capital(data_extracao), False

    def __consolidar_compras_capital(self, data_extracao):
        dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'objeto',
                            consolidacao.MOD_APLICACAO_COD: 'numero_modalidade',
                            consolidacao.ANO: 'ano_modalidade', consolidacao.MOD_APLIC_DESCRICAO: 'modalidade',
                            consolidacao.CONTRATANTE_DESCRICAO: 'orgao_nome', consolidacao.UG_DESCRICAO: 'orgao_nome',
                            consolidacao.NUMERO_PROCESSO: 'num_processo'}
        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'AL', 'portal_transparencia', 'Maceio', 'compras.xlsx'))
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Maceio
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'AL', get_codigo_municipio_por_nome('Maceió', 'AL'), data_extracao)
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Maceió'

        # Salva arquivos adicionais (informações acessórias que podem ser relevantes)
        planilha_original = os.path.join(config.diretorio_dados, 'AL', 'portal_transparencia', 'Maceio', 'compras.xlsx')

        documentos = pd.read_excel(planilha_original, sheet_name='documentos')
        documentos[consolidacao.FONTE_DADOS] = fonte_dados
        salvar(documentos, 'AL', '_Maceio_documentos')

        homologacoes = pd.read_excel(planilha_original, sheet_name='homologacoes')
        homologacoes[consolidacao.FONTE_DADOS] = fonte_dados
        salvar(homologacoes, 'AL', '_Maceio_homologacoes')

        atas = pd.read_excel(planilha_original, sheet_name='atas')
        atas[consolidacao.FONTE_DADOS] = fonte_dados
        salvar(atas, 'AL', '_Maceio_atas')

        return df


class PortalTransparencia_Maceio(JSONParser):

    def __init__(self):
        super().__init__(config.url_pt_Maceio, 'num_processo', 'compras', 'portal_transparencia', 'AL', 'Maceio')

    def _get_elemento_raiz(self, conteudo):
        return conteudo['data']
