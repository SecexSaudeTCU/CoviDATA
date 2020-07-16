import logging
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pre_processar_pt_PB(df):
    # Remove a última linha do objeto pandas DataFrame "df"
    df.drop(df.tail(1).index, inplace=True)

    # Converte as colunas de datas para objetos string
    for col in np.array(['Início', 'Final']):
        df[col] = df[col].apply(lambda x: x.strftime('%d/%m/%Y'))

    # Acrescenta a coluna "Nome Favorecido" ao objeto pandas DataFrame "df"
    df['Nome Favorecido'] = df['Contratado'].apply(lambda x: x.split(' - ')[1])
    # Acrescenta a coluna "CNPJ/CPF Favorecido" ao objeto pandas DataFrame "df"
    df['CNPJ/CPF Favorecido'] = df['Contratado'].apply(lambda x: x.split(' - ')[0])

    # Reordena as colunas do objeto pandas DataFrame "df"
    df = df[['Contrato', 'Nº Licitação', 'Início', 'Final', 'Órgão',
             'Nome Favorecido', 'CNPJ/CPF Favorecido', 'Objetivo',
             'Valor']]

    return df


def pos_processar_pt_PB(df):
    for i in range(len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) == 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


def consolidar_pt_PB(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.UG_DESCRICAO: 'Órgão',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF Favorecido',
                        consolidacao.CONTRATADO_DESCRICAO: 'Nome Favorecido',
                        consolidacao.DESPESA_DESCRICAO: 'Objetivo',
                        consolidacao.VALOR_CONTRATO: 'Valor', consolidacao.NUMERO_CONTRATO: 'Contrato'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Nº Licitação', 'Início', 'Final']

    # Lê o arquivo "xlsx" de contratos baixado como um objeto pandas DataFrame selecionando as linhas e colunas úteis
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'PB', 'portal_transparencia', 'Paraiba',
                                          'ListaContratos.xlsx'),
                                usecols=[0, 3, 4, 5, 6, 7, 9, 11],
                                skiprows=list(range(4)))

    # Chama a função "pre_processar_pt_PB" definida neste módulo
    df = pre_processar_pt_PB(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_PB, 'PB', '',
                           data_extracao, pos_processar_pt_PB)

    return df


def consolidar_pt_JoaoPessoa(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.UG_DESCRICAO: 'Unidade',
                        consolidacao.CONTRATADO_DESCRICAO: 'Nome Favorecido',
                        consolidacao.DOCUMENTO_NUMERO: 'Empenho',
                        consolidacao.DOCUMENTO_DATA: 'Data Empenho',
                        consolidacao.VALOR_EMPENHADO: 'Valor Empenhado',
                        consolidacao.VALOR_LIQUIDADO: 'Valor Liquidado',
                        consolidacao.VALOR_PAGO: 'Valor Pago',
                        consolidacao.RP_PAGO: 'Saldo a Pagar'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = []

    # Lê o arquivo "xlsx" de contratos baixado como um objeto pandas DataFrame selecionando as linhas e colunas úteis
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'PB', 'portal_transparencia', 'JoaoPessoa',
                                          'Dados_Portal_Transparencia_JoaoPessoa.xlsx'))

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_JoaoPessoa, 'PB',
                           get_codigo_municipio_por_nome('João Pessoa', 'PB'), data_extracao)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Paraíba')

    consolidacoes = consolidar_pt_PB(data_extracao)
    consolidacao_pt_JoaoPessoa = consolidar_pt_JoaoPessoa(data_extracao)

    consolidacoes = consolidacoes.append(consolidacao_pt_JoaoPessoa, ignore_index=True, sort=False)

    salvar(consolidacoes, 'PB')
