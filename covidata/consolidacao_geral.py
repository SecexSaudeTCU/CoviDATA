import os
import re
import time

import pandas as pd
import unidecode

from covidata import config
from covidata.cnpj.rfb import DAO_RFB
from covidata.persistencia import consolidacao
from covidata.cnpj.indice import buscar_por_razao_social


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
    dao_rfb = DAO_RFB()

    for i in range(0, len(df)):
        if consolidacao.CONTRATADO_DESCRICAO in df.columns and df.loc[
            i, consolidacao.FAVORECIDO_TIPO] != consolidacao.TIPO_FAVORECIDO_CPF:
            if consolidacao.CONTRATADO_CNPJ in df.columns:
                cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

                if ((type(cnpj) is str) and cnpj.strip() == '') or pd.isna(cnpj) or pd.isnull(cnpj):
                    __inferir_cnpj(dao_rfb, df, i, map_nome_cnpjs, map_nomes_cnpjs_sugeridos)
                else:
                    df.loc[i, consolidacao.CNPJ_INFERIDO] = 'NÃO'
            else:
                __inferir_cnpj(dao_rfb, df, i, map_nome_cnpjs, map_nomes_cnpjs_sugeridos)

    dao_rfb.encerrar_conexao()

    return df


def __inferir_cnpj(dao_rfb, df, i, map_nomes_exatos_cnpjs, map_nomes_cnpjs_sugeridos):
    descricao_contratado = df.loc[i, consolidacao.CONTRATADO_DESCRICAO]
    if not pd.isna(descricao_contratado) and not pd.isnull(descricao_contratado):
        descricao_contratado = __processar_descricao_contratado(descricao_contratado)

        if descricao_contratado != '':
            cnpjs = dao_rfb.buscar_cnpj_por_razao_social_ou_nome_fantasia(descricao_contratado)

            if len(cnpjs) == 1:
                df.loc[i, consolidacao.CONTRATADO_CNPJ] = cnpjs[0]
                df.loc[i, consolidacao.CNPJ_INFERIDO] = 'SIM'
            elif len(cnpjs) > 1:
                map_nomes_exatos_cnpjs[df.loc[i, consolidacao.CONTRATADO_DESCRICAO]] = cnpjs
                df.loc[i, consolidacao.CNPJ_INFERIDO] = 'VER ABA CNPJs'
            elif len(cnpjs) == 0:
                # busca no índice Lucene
                resultados_no_indice = buscar_por_razao_social(descricao_contratado)
                df.loc[i, consolidacao.CNPJ_INFERIDO] = 'VER ABA CNPJs SUGERIDOS'
                map_nomes_cnpjs_sugeridos[df.loc[i, consolidacao.CONTRATADO_DESCRICAO]] = resultados_no_indice


def __processar_descricao_contratado(descricao):
    descricao = descricao.strip()
    descricao = descricao.upper()

    # Remove espaços extras
    descricao = re.sub(' +', ' ', descricao)

    # Remove acentos
    descricao = unidecode.unidecode(descricao)

    # Remova caracteres especiais
    descricao = descricao.replace('&', '').replace('/', '').replace('-', '')

    return descricao


consolidar()
