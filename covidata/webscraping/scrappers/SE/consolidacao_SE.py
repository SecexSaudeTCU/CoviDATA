import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar



def pre_processar_pt_SE(df1, df2):

    # Renomeia colunas do objeto pandas DataFrame "df1"
    df1.rename(index=str,
               columns={'Mês': 'Mês Empenho',
                        'Valor Original (R$)': 'Empenhado Original',
                        'Valor Reforço (R$)': 'Empenhado Reforço',
                        'Valor Total (R$)': 'Empenhado'},
               inplace=True)

    # Renomeia colunas do objeto pandas DataFrame "df2"
    df2.rename(index=str,
               columns={'Mês': 'Mês Liquidação',
                        'Valor do Mês (R$)': 'Liquidado'},
               inplace=True)

    # Faz o merge de "df1" com "df2" por uma coluna tendo por base "df2"
    df = pd.merge(df1, df2[['Nº do empenho', 'Mês Liquidação', 'Liquidado']],
                  how='right',
                  left_on='Nº do empenho',
                  right_on='Nº do empenho')

    return df


def pre_processar_pt_Aracaju(df1, df2, df3):

    # Renomeia colunas do objeto pandas DataFrame "df1"
    df1.rename(index=str,
               columns={'Data': 'Data Empenho',
                        'Anulado': 'Empenhado Anulado',
                        'Reforçado': 'Empenhado Reforçado'},
               inplace=True)

    # Renomeia colunas do objeto pandas DataFrame "df2"
    df2.rename(index=str,
               columns={'Data': 'Data Liquidação',
                        'Liq': 'Liquidação',
                        'Retido': 'Liquidado Retido'},
               inplace=True)

    # Renomeia colunas do objeto pandas DataFrame "df3"
    df3.rename(index=str,
               columns={'Data': 'Data Pagamento',
                        'Retido': 'Pagamento Retido'},
               inplace=True)

    # Faz o merge de "df3" com parte de "df2" por uma coluna tendo por base "df3"
    df = pd.merge(df3, df2[['Data Liquidação', 'Empenho', 'Liquidação', 'Dotação', 'Documento', 'Liquidado', 'Liquidado Retido']],
                  how='left',
                  left_on='Empenho',
                  right_on='Empenho')

    # Faz o merge de "df" com "df1" por uma coluna tendo por base "df"
    df = pd.merge(df, df1[['Data Empenho', 'Empenho', 'Empenhado', 'Empenhado Anulado', 'Empenhado Reforçado']],
                  how='left',
                  left_on='Empenho',
                  right_on='Empenho')

    print(list(df.columns))

    return df


def pos_processar_pt(df):

    for i in range(len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) >= 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df


def consolidar_pt_SE(data_extracao):

    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Unidade',
                        consolidacao.DOCUMENTO_NUMERO: 'Nº do empenho',
                        consolidacao.PROGRAMA_DESCRICAO: 'Programa',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'Elemento',
                        consolidacao.CONTRATADO_DESCRICAO: 'Razão Social Favorecido',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ Favorecido',
                        consolidacao.VALOR_EMPENHADO: 'Empenhado',
                        consolidacao.VALOR_LIQUIDADO: 'Liquidado'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Mês Empenho', 'Empenhado Original', 'Empenhado Reforço', 'Mês Liquidação']

    # Lê o arquivo "xlsx" de nome "Dados_Portal_Transparencia_Sergipe" de contratos baixado como um objeto pandas DataFrame

    df_empenhos = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                          'Sergipe', 'Dados_Portal_Transparencia_Sergipe.xlsx'),
                                sheet_name='Empenhos')

    df_liquidacoes = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                             'Sergipe', 'Dados_Portal_Transparencia_Sergipe.xlsx'),
                                   sheet_name='Liquidações')

    # Chama a função "pre_processar_pt_SE" definida neste módulo
    df = pre_processar_pt_SE(df_empenhos, df_liquidacoes)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SE, 'SE', '',
                           data_extracao, pos_processar_pt)
    return df


def consolidar_pt_Aracaju(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                        consolidacao.UG_DESCRICAO: 'Unidade',
                        consolidacao.DOCUMENTO_NUMERO: 'Empenho',
                        consolidacao.CONTRATADO_DESCRICAO: 'Nome Favorecido',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF Favorecido',
                        consolidacao.VALOR_PAGO: 'Pago',
                        consolidacao.DESPESA_DESCRICAO: 'DsEmpenho',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'DsItemDespesa',
                        consolidacao.FONTE_RECURSOS_COD: 'Dotação',
                        consolidacao.VALOR_LIQUIDADO: 'Liquidado',
                        consolidacao.VALOR_EMPENHADO: 'Empenhado'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Data Pagamento', 'Processo', 'Pagamento Retido', 'Nota de Pagamento',
                          'Data Liquidação', 'Liquidação', 'Documento', 'Liquidado Retido',
                          'Data Empenho', 'Empenhado Anulado', 'Empenhado Reforçado']

    df_empenhos = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                          'Aracaju', 'Dados_Portal_Transparencia_Aracaju.xlsx'),
                                sheet_name='Empenhos')

    df_liquidacoes = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                             'Aracaju', 'Dados_Portal_Transparencia_Aracaju.xlsx'),
                                   sheet_name='Liquidações')

    df_pagamentos = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                            'Aracaju', 'Dados_Portal_Transparencia_Aracaju.xlsx'),
                                  sheet_name='Pagamentos')

    df = pre_processar_pt_Aracaju(df_empenhos, df_liquidacoes, df_pagamentos)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Aracaju, 'SE',
                           get_codigo_municipio_por_nome('Aracaju', 'SE'), data_extracao, pos_processar_pt)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Sergipe')

    consolidacoes = consolidar_pt_SE(data_extracao)
    consolidacao_pt_Aracaju = consolidar_pt_Aracaju(data_extracao)

    consolidacoes = consolidacoes.append(consolidacao_pt_Aracaju, ignore_index=True, sort=False)

    salvar(consolidacoes, 'SE')