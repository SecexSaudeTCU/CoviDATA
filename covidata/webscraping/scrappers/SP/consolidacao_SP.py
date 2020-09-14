import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pre_processar_tcm(df):
    # Renomeia as colunas especificadas
    df.rename(index=str,
              columns={'IdLicitacao': 'Identificador Licitação',
                       'Modalidade': 'Modalidade Licitação',
                       'Dt. Publicação': 'Data Publicação',
                       'Licitação': 'Número Licitação',
                       'Processo Externo': 'Número Processo'},
              inplace=True)

    return df


def consolidar_tcm(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto',
                        consolidacao.VALOR_CONTRATO: 'Valor'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Identificador Licitação', 'Modalidade Licitação', 'Publicação',
                          'Data Publicação', 'Unidade', 'Número Licitação', 'Número Processo']

    # Lê o arquivo "csv" de licitações baixado como um objeto pandas DataFrame
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'SP', 'tcm', 'licitacoes.xls'),
                                skiprows=list(range(4)),
                                index_col=0)

    # Chama a função "pre_processar_tcm" definida neste módulo
    df = pre_processar_tcm(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_TCM + ' - ' + config.url_tcm_SP, 'SP',
                           get_codigo_municipio_por_nome('São Paulo', 'SP'), data_extracao)

    return df


def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Sâo Paulo')

    # TODO: Indisponível/instável
    # consolidacao_tcm = consolidar_tcm(data_extracao)
    # consolidacoes = consolidacoes.append(consolidacao_tcm, ignore_index=True, sort=False)

    salvar(df_consolidado, 'SP')

