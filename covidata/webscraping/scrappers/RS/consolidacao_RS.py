import logging
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar



def pre_processar_tce(df):

    # Lê o primeiro elemento do objeto list que se constitui em um objeto pandas DataFrame
    df = df[0]

    # Elimina a primeira linha do objeto pandas DataFrame "df"
    df.drop(0, axis=0, inplace=True)

    # Reseta o index de "df"
    df.reset_index(drop=True, inplace=True)

    # Renomeia as colunas especificadas
    df.rename(index=str,
              columns={0: 'Órgão',
                       1: 'Modalidade Licitação',
                       2: 'Número Licitação',
                       3: 'Ano',
                       4: 'Processo',
                       5: 'Objeto',
                       6: 'Valor Homologado',
                       7: 'Resultado Licitação',
                       8: 'Vencedor Licitação'},
              inplace=True)

    return df


def consolidar_pt_RS(data_extracao):

    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Central Compras',
                        consolidacao.DESPESA_DESCRICAO: 'Descrição Item'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Número Licitação', 'Tipo Objeto', 'Modalidade Licitação', 'Data Abertura',
                          'Situação Oferta', 'Número Lote', 'Situação Lote', 'Número Item',
                          'Quantidade', 'Preço Unitário', 'Preço Total', 'Nome Arrematante',
                          'CNPJ Arrematante', 'URL Documentos', 'URL Propostas', 'Unidade Valor',
                          'Data Homologação']

    # Lê o arquivo "xlsx" de licitações baixado como um objeto pandas DataFrame
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'RS', 'portal_transparencia',
                                'RioGrandeSul', 'Dados_Portal_Transparencia_RioGrandeSul.xlsx'))

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RS, 'RS', '',
                           data_extracao)

    return df


def consolidar_tce(data_extracao):
    # Objeto dict em que os valores têm chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Modalidade Licitação', 'Número Licitação', 'Ano', 'Processo',
                          'Valor Homologado', 'Resultado Licitação', 'Vencedor Licitação']

    # Lê o arquivo "xls" de licitações baixado como um objeto list utilizando a função "read_html" da biblioteca pandas
    df_original = pd.read_html(path.join(config.diretorio_dados, 'RS', 'tce', 'licitações_-_covid-19.xls'),
                               decimal=',')

    # Chama a função "pre_processar_tce" definida neste módulo
    df = pre_processar_tce(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_RS, 'RS', '', data_extracao)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Rio Grande do Sul')

    consolidacoes = consolidar_pt_RS(data_extracao)
    consolidacao_tce = consolidar_tce(data_extracao)

    consolidacoes = consolidacoes.append(consolidacao_tce, ignore_index=True, sort=False)

    salvar(consolidacoes, 'RS')
