import datetime

import logging
import os
from glob import glob
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_consolidar_dispensas(df):
    # Elimina a última linha, que só contém um totalizador
    df = df.drop(df.index[-1])

    df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Recife'
    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    # Unifica colunas com nomes parecidos
    if len(df['DATA DE EMPENHO\nCONTRATO'].value_counts()) > 0:
        df[consolidacao.DOCUMENTO_DATA] = df['DATA DE EMPENHO\nCONTRATO']
    elif len(df['DATA DE EMPENHO/\nCONTRATO'].value_counts()) > 0:
        df[consolidacao.DOCUMENTO_DATA] = df['DATA DE EMPENHO/\nCONTRATO']

    df = df.drop(['DATA DE EMPENHO\nCONTRATO'], axis=1)
    df = df.drop(['DATA DE EMPENHO/\nCONTRATO'], axis=1)

    return df


def __consolidar_dispensas(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.UG_DESCRICAO: 'Órgão',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.CONTRATADO_DESCRICAO: 'Nome Fornecedor',
                        consolidacao.VALOR_CONTRATO: 'Valor por Fornecedor\n(R$)',
                        consolidacao.DATA_FIM_VIGENCIA: 'Data Vigência',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local de Execução'}
    colunas_adicionais = ['Nº Dispensa', 'Anulação/ Revogação/ Retificação/\nSuspensão', 'Data de Empenho/\nContrato',
                          'Data de Empenho\nContrato']
    df_final = pd.DataFrame()

    # Processando arquivos Excel
    planilhas = [y for x in os.walk(path.join(config.diretorio_dados, 'PE', 'portal_transparencia', 'Recife')) for y in
                 glob(os.path.join(x[0], '*.xlsx'))]

    for planilha_original in planilhas:
        df_original = pd.read_excel(planilha_original)
        df = __processar_df_original(colunas_adicionais, data_extracao, df_original, dicionario_dados)
        df_final = df_final.append(df)

    # Processando arquivos ODS
    #TODO: Parou de funcionar
    """
    planilhas = [y for x in os.walk(path.join(config.diretorio_dados, 'PE', 'portal_transparencia', 'Recife')) for y in
                 glob(os.path.join(x[0], '*.ods'))]

    for planilha_original in planilhas:
        df_original = pd.read_excel(planilha_original, engine='odf')
        df = __processar_df_original(colunas_adicionais, data_extracao, df_original, dicionario_dados)
        df_final = df_final.append(df)
    """
    return df_final


def __processar_df_original(colunas_adicionais, data_extracao, df_original, dicionario_dados):
    # Procura pelo cabeçalho:
    mask = np.column_stack([df_original[col].str.contains(colunas_adicionais[0], na=False) for col in df_original])
    df_original.columns = df_original[mask].values.tolist()[0]
    # Remove as linhas anteriores ao cabeçalho
    while (df_original.iloc[0, 0] != df_original.columns[0]):
        df_original = df_original.drop(df_original.index[0])
    df_original = df_original.drop(df_original.index[0])
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Recife
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'PE', get_codigo_municipio_por_nome('Recife', 'PE'), data_extracao,
                           pos_processar_consolidar_dispensas)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Pernambuco')
    dispensas = __consolidar_dispensas(data_extracao)
    salvar(dispensas, 'PE')

#consolidar(datetime.datetime.now())