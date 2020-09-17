import json

import os
import requests
import time

import pandas as pd

from covidata import config
from covidata.cnpj import cnpj_util
from covidata.persistencia import consolidacao


def consolidar():
    start_time = time.time()
    diretorio = os.path.join(config.diretorio_dados, 'consolidados')

    # Especifica uma ordenação padrão mínima para as colunas.
    df_final = pd.DataFrame(columns=[consolidacao.FONTE_DADOS, consolidacao.DATA_EXTRACAO_DADOS, consolidacao.ESFERA,
                                     consolidacao.UF, consolidacao.COD_IBGE_MUNICIPIO, consolidacao.MUNICIPIO_DESCRICAO,
                                     consolidacao.CONTRATADO_CNPJ, consolidacao.CONTRATADO_DESCRICAO,
                                     consolidacao.CNPJ_INFERIDO])
    map_nomes_exatos_cnpjs = dict()
    map_nomes_cnpjs_sugeridos = dict()

    for root, subdirs, files in os.walk(diretorio):
        for file in files:
            if len(file) <= 7:
                print('Lendo arquivo ' + file)
                df = pd.read_excel(os.path.join(root, file))
                df = __processar_uf(df, map_nomes_exatos_cnpjs, map_nomes_cnpjs_sugeridos)
                df_final = df_final.append(df)

    writer = pd.ExcelWriter(os.path.join(diretorio, 'UFs.xlsx'), engine='xlsxwriter')
    df_final.to_excel(writer, sheet_name='Dados consolidados')

    df_map_nomes_exatos_cnpjs = pd.concat({k: pd.Series(v) for k, v in map_nomes_exatos_cnpjs.items()})
    df_map_nomes_exatos_cnpjs.to_excel(writer, sheet_name='CNPJs')

    df_map_nomes_cnpjs_sugeridos = pd.concat(
        {k: pd.DataFrame(v, columns=['RAZÃO SOCIAL SUGERIDA', 'CNPJ']) for k, v in map_nomes_cnpjs_sugeridos.items()})
    df_map_nomes_cnpjs_sugeridos.to_excel(writer, sheet_name='CNPJs sugeridos')

    writer.save()

    print('Total de registros: %s' % len(df_final))
    print('Total de registros do tipo PJ: %s' % len(
        df_final[df_final[consolidacao.FAVORECIDO_TIPO] != consolidacao.TIPO_FAVORECIDO_CPF]))
    print('Total de registros com CNPJ originalmente preenchido: %s' % len(
        df_final[df_final[consolidacao.CNPJ_INFERIDO] == 'NÃO']))
    print('Total de registros com CNPJ inferido: %s' % len(df_final[df_final[consolidacao.CNPJ_INFERIDO] == 'SIM']))
    print('Total de registros com mais de um CNPJ associado ao nome do contratado: %s' % len(
        df_final[df_final[consolidacao.CNPJ_INFERIDO] == 'VER ABA CNPJs']))

    print("--- %s segundos ---" % (time.time() - start_time))


def __processar_uf(df, map_nome_cnpjs, map_nomes_cnpjs_sugeridos):
    df = df.drop(columns='Unnamed: 0', axis=1, errors='ignore')
    #    dao_rfb = DAO_RFB()

    for i in range(0, len(df)):
        if consolidacao.CONTRATADO_DESCRICAO in df.columns and df.loc[
            i, consolidacao.FAVORECIDO_TIPO] != consolidacao.TIPO_FAVORECIDO_CPF:
            if consolidacao.CONTRATADO_CNPJ in df.columns:
                cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

                if ((type(cnpj) is str) and cnpj.strip() == '') or pd.isna(cnpj) or pd.isnull(cnpj):
                    __inferir_cnpj(df, i, map_nome_cnpjs, map_nomes_cnpjs_sugeridos)
                else:
                    df.loc[i, consolidacao.CNPJ_INFERIDO] = 'NÃO'
            else:
                __inferir_cnpj(df, i, map_nome_cnpjs, map_nomes_cnpjs_sugeridos)

    # dao_rfb.encerrar_conexao()

    return df


def __inferir_cnpj(df, i, map_nomes_exatos_cnpjs, map_nomes_cnpjs_sugeridos):
    descricao_contratado = df.loc[i, consolidacao.CONTRATADO_DESCRICAO]

    if not pd.isna(descricao_contratado) and not pd.isnull(descricao_contratado):
        map_empresa_to_cnpjs, tipo_busca = __buscar_empresas_por_razao_social(descricao_contratado)

        if tipo_busca == 'BUSCA EXATA RFB':
            empresa, cnpjs = list(map_empresa_to_cnpjs.items())[0]

            if len(cnpjs) == 1:
                df.loc[i, consolidacao.CONTRATADO_CNPJ] = cnpjs[0]
                df.loc[i, consolidacao.CNPJ_INFERIDO] = 'SIM'
            elif len(cnpjs) > 1:
                map_nomes_exatos_cnpjs[df.loc[i, consolidacao.CONTRATADO_DESCRICAO]] = cnpjs
                df.loc[i, consolidacao.CNPJ_INFERIDO] = 'VER ABA CNPJs'
        else:
            df.loc[i, consolidacao.CNPJ_INFERIDO] = 'VER ABA CNPJs SUGERIDOS'
            resultados_no_indice = []

            for empresa, cnpjs in map_empresa_to_cnpjs.items():
                for cnpj in cnpjs:
                    resultados_no_indice.append((empresa, cnpj))

            map_nomes_cnpjs_sugeridos[df.loc[i, consolidacao.CONTRATADO_DESCRICAO]] = resultados_no_indice


def __buscar_empresas_por_razao_social(razao_social):
    resultado = json.loads(requests.get(config.url_api_cnpj + razao_social).content)
    map_empresa_to_cnpjs = resultado['dados']['empresas']
    tipo_busca = resultado['dados']['tipo_busca']
    return map_empresa_to_cnpjs, tipo_busca


consolidar()
