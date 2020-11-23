import numpy as np
import os
import pandas as pd
import time
from cnpjutil.cnpj.fabrica_provedor_cnpj import get_repositorio_cnpj

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.disco_virtual import salvar


def consolidar():
    start_time = time.time()
    diretorio = os.path.join(config.diretorio_dados, 'consolidados')

    # Especifica uma ordenação padrão mínima para as colunas.
    df_final = pd.DataFrame(columns=[consolidacao.FONTE_DADOS, consolidacao.DATA_EXTRACAO_DADOS, consolidacao.ESFERA,
                                     consolidacao.UF, consolidacao.COD_IBGE_MUNICIPIO, consolidacao.MUNICIPIO_DESCRICAO,
                                     consolidacao.CONTRATADO_CNPJ, consolidacao.CONTRATADO_DESCRICAO,
                                     consolidacao.CNPJ_INFERIDO])

    repositorio_cnpj = get_repositorio_cnpj(str(config.arquivo_config_cnpj))

    for root, subdirs, files in os.walk(diretorio):
        for file in files:
            if len(file) <= 7:
                print('Lendo arquivo ' + file)
                df = pd.read_excel(os.path.join(root, file))
                df = __processar_uf(df, repositorio_cnpj)
                df_final = df_final.append(df)

    # Remove a notação científica
    __remover_notacao_cientifica(df_final)

    __salvar(df_final, diretorio)

    print('Total de registros: %s' % len(df_final))
    print('Total de registros com CNPJ originalmente preenchido: %s' % len(
        df_final[df_final[consolidacao.CNPJ_INFERIDO] == 'NÃO']))
    print('Total de registros com CNPJ inferido: %s' % len(df_final[df_final[consolidacao.CNPJ_INFERIDO] == 'SIM']))

    print("--- %s segundos ---" % (time.time() - start_time))


def __salvar(df_final, diretorio):
    writer = pd.ExcelWriter(os.path.join(diretorio, 'WEBSCRAPING.xlsx'), engine='xlsxwriter')
    df_final.to_excel(writer, sheet_name='Dados')
    writer.save()
    # Salva em disco virtual
    salvar(caminho_arquivo=config.diretorio_dados.joinpath('consolidados').joinpath('WEBSCRAPING.xlsx'),
           nome_arquivo='WEBSCRAPING.xlsx')


def __remover_notacao_cientifica(df_final):
    df_final[consolidacao.CONTRATADO_CNPJ] = df_final[consolidacao.CONTRATADO_CNPJ].apply(
        lambda x: '{:.0f}'.format(x) if type(x) == float else str(x))
    df_final[consolidacao.CONTRATADO_CNPJ] = df_final[consolidacao.CONTRATADO_CNPJ].fillna('')
    df_final[consolidacao.CONTRATADO_CNPJ] = df_final[consolidacao.CONTRATADO_CNPJ].replace(np.nan, '', regex=True)
    df_final[consolidacao.CONTRATADO_CNPJ] = df_final[consolidacao.CONTRATADO_CNPJ].replace('nan', '', regex=True)
    df_final[consolidacao.DOCUMENTO_NUMERO] = df_final[consolidacao.DOCUMENTO_NUMERO].apply(
        lambda x: '{:.0f}'.format(x) if type(x) == float else str(x))
    df_final[consolidacao.DOCUMENTO_NUMERO] = df_final[consolidacao.DOCUMENTO_NUMERO].fillna('')
    df_final[consolidacao.DOCUMENTO_NUMERO] = df_final[consolidacao.DOCUMENTO_NUMERO].replace(np.nan, '', regex=True)
    df_final[consolidacao.DOCUMENTO_NUMERO] = df_final[consolidacao.DOCUMENTO_NUMERO].replace('nan', '', regex=True)


def __processar_uf(df, repositorio_cnpj):
    df = df.drop(columns='Unnamed: 0', axis=1, errors='ignore')
    df = df.drop(columns='temp', axis=1, errors='ignore')

    for i in range(0, len(df)):
        if consolidacao.CONTRATADO_DESCRICAO in df.columns:
            if consolidacao.CONTRATADO_CNPJ in df.columns:
                cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

                if ((type(cnpj) is str) and cnpj.strip() == '') or pd.isna(cnpj) or pd.isnull(cnpj):
                    __inferir_cnpj(df, i, repositorio_cnpj)
                else:
                    df.loc[i, consolidacao.CNPJ_INFERIDO] = 'NÃO'
            else:
                __inferir_cnpj(df, i, repositorio_cnpj)

    return df


def __inferir_cnpj(df, i, repositorio_cnpj):
    descricao_contratado = df.loc[i, consolidacao.CONTRATADO_DESCRICAO]

    if not pd.isna(descricao_contratado) and not pd.isnull(descricao_contratado):
        map_empresa_to_cnpjs, tipo_busca = repositorio_cnpj.buscar_empresas_por_razao_social(descricao_contratado)
        lista = list(map_empresa_to_cnpjs.items())

        if len(lista) > 0:
            empresa, cnpjs = lista[0]

            if len(cnpjs) == 1:
                df.loc[i, consolidacao.CONTRATADO_CNPJ] = cnpjs[0]
                df.loc[i, consolidacao.CNPJ_INFERIDO] = 'SIM'
