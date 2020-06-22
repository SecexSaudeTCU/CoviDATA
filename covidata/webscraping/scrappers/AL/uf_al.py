import json
import os
import time
import urllib.request
from os import path

import pandas as pd

import config
from webscraping.downloader import FileDownloader


def pt_Maceio():
    with urllib.request.urlopen(config.url_pt_Maceio) as url:
        diretorio = path.join(config.diretorio_dados, 'AL', 'portal_transparencia', 'Maceio')
        if not path.exists(diretorio):
            os.makedirs(diretorio)

        data = __processar_e_salvar_json(diretorio, url)

        colunas_df_principal = []
        linhas_df_principal = []

        dfs_auxiliares = dict()

        for elemento in data:
            id = 'num_processo'
            valor_id = elemento[id]
            linha = []

            for key in elemento.keys():
                if isinstance(elemento[key], list):
                    # cria um dataframe/aba com os elementos da lista
                    if len(elemento[key]) > 0:
                        lista_auxiliar = elemento[key]
                        colunas_df_auxiliar = [id]
                        linhas_df_auxiliar = []

                        for elemento_auxiliar in lista_auxiliar:
                            linha_df_auxiliar = [valor_id]

                            for key2 in elemento_auxiliar.keys():
                                __processar_linha_univalorada(colunas_df_auxiliar, elemento_auxiliar, key2,
                                                              linha_df_auxiliar)

                            linhas_df_auxiliar.append(linha_df_auxiliar)

                        df_auxiliar = pd.DataFrame(linhas_df_auxiliar, columns=colunas_df_auxiliar)
                        if not key in dfs_auxiliares:
                            dfs_auxiliares[key] = df_auxiliar
                        else:
                            dfs_auxiliares[key] = dfs_auxiliares[key].append(df_auxiliar)

                else:
                    __processar_linha_univalorada(colunas_df_principal, elemento, key, linha)

            linhas_df_principal.append(linha)

        df_principal = pd.DataFrame(linhas_df_principal, columns=colunas_df_principal)

        __salvar_como_planilha(df_principal, dfs_auxiliares, diretorio)


def __processar_e_salvar_json(diretorio, url):
    conteudo = url.read().decode()
    dados = json.loads(conteudo)
    data = dados['data']
    with open(os.path.join(diretorio, 'compras.json'), 'w') as f:
        f.write(conteudo)
    return data


def __salvar_como_planilha(df_principal, dfs_auxiliares, diretorio_principal):
    writer = pd.ExcelWriter(os.path.join(diretorio_principal, 'compras.xlsx'), engine='xlsxwriter')
    df_principal.to_excel(writer, sheet_name='compras')
    for nome, df in dfs_auxiliares.items():
        df.to_excel(writer, sheet_name=nome)
    writer.save()


def __processar_linha_univalorada(colunas_df, elemento, key, linha):
    if isinstance(elemento[key], dict):
        dicionario = elemento[key]
        for key2 in dicionario.keys():
            nome_coluna = key + '_' + key2
            if not nome_coluna in colunas_df:
                colunas_df.append(nome_coluna)
            linha.append(dicionario[key2])
    else:
        if not key in colunas_df:
            colunas_df.append(key)
        linha.append(elemento[key])



def main():
    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_AL = FileDownloader(path.join(config.diretorio_dados, 'AL', 'portal_transparencia'), config.url_pt_AL,
                           'DESPESAS COM COVID-19.xls')
    pt_AL.download()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência da capital...')
    start_time = time.time()
    pt_Maceio()
    print("--- %s segundos ---" % (time.time() - start_time))
