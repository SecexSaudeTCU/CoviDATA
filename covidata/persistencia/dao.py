import os
from os import path

import pandas as pd
from covidata import config


def persistir(df, fonte, nome, uf, cidade=''):
    """
    Persiste um dataframe.
    :param df: O dataframe a ser persistido.
    :param fonte: O nome da fonte dos dados (ex.: 'tce', 'tcm', 'portal_transparencia')
    :param nome: Nome que representa as informações a serem persistidas (ex.: contratos)
    :param uf: Unidade da federação.
    :param cidade: Cidade (opcional).
    :return:
    """
    diretorio = __criar_diretorio(fonte, uf, cidade)
    writer = pd.ExcelWriter(os.path.join(diretorio, nome + '.xls'), engine='xlwt')
    df.to_excel(writer, sheet_name='Sheet1', startrow=4)
    writer.save()


def persistir_dados_hierarquicos(df_principal, dfs_auxiliares, fonte, nome, uf, cidade=''):
    """
    Persiste dados em uma hierarquia master-detail.
    :param df_principal: O dataframe principal a ser persistido (ie.: tabela principal).
    :param dfs_auxiliares: Os dataframe auxiliares/filhos (ie.:tabelas auxiliares).
    :param fonte: O nome da fonte dos dados (ex.: 'tce', 'tcm', 'portal_transparencia')
    :param nome: Nome que representa as informações a serem persistidas (ex.: contratos)
    :param uf: Unidade da federação.
    :param cidade: Cidade (opcional).
    :return:
    """
    diretorio = __criar_diretorio(fonte, uf, cidade)
    writer = pd.ExcelWriter(os.path.join(diretorio, nome + '.xlsx'), engine='xlsxwriter')
    df_principal.to_excel(writer, sheet_name=nome)
    for nome, df in dfs_auxiliares.items():
        df.to_excel(writer, sheet_name=nome)
    writer.save()


def __criar_diretorio(fonte, uf, cidade):
    diretorio = path.join(config.diretorio_dados, uf, fonte, cidade)
    if not path.exists(diretorio):
        os.makedirs(diretorio)
    return diretorio
