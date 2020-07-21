import datetime
import logging
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar



def pre_processar_pt_MT(df):

    # Lê o primeiro elemento do objeto list que se constitui em um objeto pandas DataFrame
    df = df[0]

    # Renomeia as colunas especificadas
    df.rename(columns={'Item do Objeto': 'Item',
                       'Unidade': 'Unidade Item',
                       'Quantidade': 'Quantidade Item',
                       'Valor Unitario': 'PU Item',
                       'Valor Global': 'Preço Item',
                       'Modalidade de Contratacao': 'Modalidade Contratação'},
              inplace=True)

    # Insere a substring "," na penúltima posição dos objetos string que compõem...
    # as colunas especificadas
    for col in np.array(['PU Item', 'Preço Item']):
        df[col] = df[col].apply(lambda x: x[:-2] + ',' + x[-2:])

    return df



def pos_consolidar_pt_MT(df):
    try:
        df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
        df = df.astype({consolidacao.CONTRATADO_CNPJ: str})
    except ValueError:
        #Há linhas com dados inválidos para CNPJ (ex.: data)
        df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    for i in range(0, len(df)):
        tamanho = len(df.loc[i, consolidacao.CONTRATADO_CNPJ])

        if tamanho > 2:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

            if tamanho < 14:
                df.loc[i, consolidacao.CONTRATADO_CNPJ] = '0' * (14 - tamanho) + df.loc[i, consolidacao.CONTRATADO_CNPJ]

    return df


def __consolidar_pt_MT(data_extracao):
    # Objeto dict em que os valores têm chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Entidade',
                        consolidacao.UG_DESCRICAO: 'Entidade',
                        consolidacao.NUMERO_PROCESSO: 'Numerodo Processo',
                        consolidacao.NUMERO_CONTRATO: 'Contrato',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.CONTRATADO_DESCRICAO: 'Razao Social',
                        consolidacao.DATA_CELEBRACAO: 'Data de Celebracao',
                        consolidacao.DATA_VIGENCIA: 'Data de Vigencia',
                        consolidacao.SITUACAO: 'Situacao',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local da Execucao'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Item', 'Unidade Item', 'Quantidade Item', 'PU Item', 'Preço Item',
                          'Modalidade Contratação']

    # Lê o arquivo "xls" de contratos baixado como um objeto list utilizando a função "read_html" da biblioteca pandas
    df_original = pd.read_html(path.join(config.diretorio_dados, 'MT', 'portal_transparencia',
                                          'transparencia_excel.xls'))

    # Chama a função "pre_processar_tce" definida neste módulo
    df = pre_processar_pt_MT(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MT,
                           'MT', '', data_extracao, pos_consolidar_pt_MT)

    return df


def __consolidar_pt_Cuiaba(data_extracao):

    pass


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Mato Grosso')

    consolidacoes = __consolidar_pt_MT(data_extracao)
    # consolidacao_pt_Cuiaba = __consolidar_pt_Cuiaba(data_extracao)
    #
    # consolidacoes = consolidacoes.append(consolidacao_pt_Cuiaba, ignore_index=True, sort=False)

    salvar(consolidacoes, 'MT')
