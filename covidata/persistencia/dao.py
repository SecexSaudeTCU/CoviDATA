import os
from os import path

import pandas as pd

import config


def persistir(df, fonte, nome, uf, cidade=''):
    diretorio = criar_diretorio(fonte, uf, cidade)

    df.to_excel(path.join(diretorio, nome + '.xlsx'))

def persistir_dados_hierarquicos(df_principal, dfs_auxiliares, fonte, nome, uf, cidade):
    diretorio = criar_diretorio(fonte, uf, cidade)
    writer = pd.ExcelWriter(os.path.join(diretorio, nome + '.xlsx'), engine='xlsxwriter')
    df_principal.to_excel(writer, sheet_name=nome)
    for nome, df in dfs_auxiliares.items():
        df.to_excel(writer, sheet_name=nome)
    writer.save()


def criar_diretorio(fonte, uf, cidade):
    diretorio = path.join(config.diretorio_dados, uf, fonte, cidade)
    if not path.exists(diretorio):
        os.makedirs(diretorio)
    return diretorio


