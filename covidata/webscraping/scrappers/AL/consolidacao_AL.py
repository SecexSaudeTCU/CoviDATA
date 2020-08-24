import logging
import os
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar
from covidata.municipios.ibge import get_codigo_municipio_por_nome


def __consolidar_compras_capital(data_extracao):
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'objeto', consolidacao.MOD_APLICACAO_COD: 'numero_modalidade',
                        consolidacao.ANO: 'ano_modalidade', consolidacao.MOD_APLIC_DESCRICAO: 'modalidade',
                        consolidacao.CONTRATANTE_DESCRICAO: 'orgao_nome', consolidacao.UG_DESCRICAO: 'orgao_nome',
                        consolidacao.NUMERO_PROCESSO: 'num_processo'}
    colunas_adicionais = ['data_abertura', 'hora_abertura', 'data_fechamento', 'hora_fechamento', 'orgao_sigla', 'cota',
                          'status', 'responsavel']
    df_original = pd.read_excel(
        path.join(config.diretorio_dados, 'AL', 'portal_transparencia', 'Maceio', 'compras.xlsx'))
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Maceio
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
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


def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Alagoas')

    compras_capital = __consolidar_compras_capital(data_extracao)
    df_consolidado = df_consolidado.append(compras_capital)

    salvar(df_consolidado, 'AL')
