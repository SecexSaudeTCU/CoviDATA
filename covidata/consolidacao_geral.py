import json
import os
import pandas as pd
import requests
import time
import numpy as np
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

    for root, subdirs, files in os.walk(diretorio):
        for file in files:
            if len(file) <= 7:
                print('Lendo arquivo ' + file)
                df = pd.read_excel(os.path.join(root, file))
                df = __processar_uf(df)
                df_final = df_final.append(df)

    writer = pd.ExcelWriter(os.path.join(diretorio, 'UFs.xlsx'), engine='xlsxwriter')

    # Remove a notação científica
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

    df_final.to_excel(writer, sheet_name='Dados consolidados')

    writer.save()

    # Salva em disco virtual
    salvar(caminho_arquivo=config.diretorio_dados.joinpath('consolidados').joinpath('UFs.xlsx'),
           nome_arquivo='UFs.xlsx')

    print('Total de registros: %s' % len(df_final))
    print('Total de registros com CNPJ originalmente preenchido: %s' % len(
        df_final[df_final[consolidacao.CNPJ_INFERIDO] == 'NÃO']))
    print('Total de registros com CNPJ inferido: %s' % len(df_final[df_final[consolidacao.CNPJ_INFERIDO] == 'SIM']))

    print("--- %s segundos ---" % (time.time() - start_time))


def __processar_uf(df):
    df = df.drop(columns='Unnamed: 0', axis=1, errors='ignore')
    df = df.drop(columns='temp', axis=1, errors='ignore')

    for i in range(0, len(df)):
        if consolidacao.CONTRATADO_DESCRICAO in df.columns:
            if consolidacao.CONTRATADO_CNPJ in df.columns:
                cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

                if ((type(cnpj) is str) and cnpj.strip() == '') or pd.isna(cnpj) or pd.isnull(cnpj):
                    __inferir_cnpj(df, i)
                else:
                    df.loc[i, consolidacao.CNPJ_INFERIDO] = 'NÃO'
            else:
                __inferir_cnpj(df, i)

    return df


def __inferir_cnpj(df, i):
    descricao_contratado = df.loc[i, consolidacao.CONTRATADO_DESCRICAO]

    if not pd.isna(descricao_contratado) and not pd.isnull(descricao_contratado):
        map_empresa_to_cnpjs, tipo_busca = __buscar_empresas_por_razao_social(descricao_contratado)
        lista = list(map_empresa_to_cnpjs.items())

        if len(lista) > 0:
            empresa, cnpjs = lista[0]

            if len(cnpjs) == 1:
                df.loc[i, consolidacao.CONTRATADO_CNPJ] = cnpjs[0]
                df.loc[i, consolidacao.CNPJ_INFERIDO] = 'SIM'


def __buscar_empresas_por_razao_social(razao_social):
    resultado = json.loads(requests.get(config.url_api_cnpj + razao_social).content)
    map_empresa_to_cnpjs = resultado['dados']['empresas']
    tipo_busca = resultado['dados']['tipo_busca']
    return map_empresa_to_cnpjs, tipo_busca


# if __name__ == '__main__':
#     consolidar()
