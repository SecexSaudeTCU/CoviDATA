import logging
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pre_processar_pt_RR(df):
    # Lê o segundo elemento do objeto list que se constitui em um objeto pandas DataFrame
    df = df[1]

    # Elimina o nível mais alto dos nomes de colunas de "df"
    df.columns = df.columns.droplevel()

    # Reescreve as colunas de valores monetários inserindo a vírgula designativa de decimais
    for col in np.array(['Empenhado', 'Liquidado', 'Pago']):
        df[col] = df[col].apply(lambda x: x[:-2] + ',' + x[-2:])

    # Renomeia as colunas especificadas
    df.rename(index=str,
              columns={'Inicio': 'Data Início',
                       'Témino': 'Data Término',
                       'Valor': 'Valor Contratual',
                       'Situação': 'Situação Contratual'},
              inplace=True)

    return df


def pos_processar_pt_RR(df):
    for i in range(len(df)):
        cpf_cnpj = df.loc[str(i), consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) >= 14:
            df.loc[str(i), consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[str(i), consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df


def pos_processar_pt_BoaVista(df):
    for i in range(len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(str(cpf_cnpj)) >= 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df


def consolidar_pt_RR(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Histórico do Pedido de Empenho',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF',
                        consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                        consolidacao.VALOR_CONTRATO: 'Valor Contratual',
                        consolidacao.VALOR_EMPENHADO: 'Empenhado',
                        consolidacao.VALOR_LIQUIDADO: 'Liquidado',
                        consolidacao.VALOR_PAGO: 'Pago', consolidacao.DATA_INICIO_VIGENCIA: 'Data Início',
                        consolidacao.DATA_FIM_VIGENCIA: 'Data Término'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Processo', 'Situação do Processo', 'Contrato', 'Tipo Contrato', 'Situação Contratual',
                          'Aditivos']

    # Lê o arquivo "xls" de contratos baixado como um objeto list utilizando a função "read_html" da biblioteca pandas
    df_original = pd.read_html(path.join(config.diretorio_dados, 'RR', 'portal_transparencia',
                                         'Roraima', 'Dados_Portal_Transparencia_Roraima.xls'),
                               decimal=',')

    # Chama a função "pre_processar_pt_RR" definida neste módulo
    df = pre_processar_pt_RR(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RR, 'RR', '',
                           data_extracao, pos_processar_pt_RR)

    return df


def consolidar_pt_BoaVista(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Objeto Licitação',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.CONTRATADO_DESCRICAO: 'Contratado',
                        consolidacao.VALOR_CONTRATO: 'Valor Contrato', consolidacao.DATA_CELEBRACAO: 'Data Contrato'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Número Licitação', 'Situação Licitação', 'Modalidade Licitacao',
                          'Data Abertura', 'Data Publicação', 'Descrição Produto',
                          'Quantidade Produto', 'PU Produto', 'Prazo Execução']

    # Lê o arquivo "xlsx" de despesas baixado como um objeto pandas DataFrame
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'RR', 'portal_transparencia',
                                          'BoaVista', 'Dados_Portal_Transparencia_BoaVista.xlsx'))

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_BoaVista, 'RR',
                           get_codigo_municipio_por_nome('Boa Vista', 'RR'), data_extracao, pos_processar_pt_BoaVista)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Roraima')

    consolidacoes = consolidar_pt_RR(data_extracao)
    consolidacao_pt_BoaVista = consolidar_pt_BoaVista(data_extracao)

    consolidacoes = consolidacoes.append(consolidacao_pt_BoaVista, ignore_index=True, sort=False)

    salvar(consolidacoes, 'RR')
