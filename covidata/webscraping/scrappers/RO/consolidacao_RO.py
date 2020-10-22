from os import path
import logging

import numpy as np
import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_consolidar_pt_RO(df):
    # Remove notação científica
    df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    for i in range(0, len(df)):
        tamanho = len(df.loc[i, consolidacao.CONTRATADO_CNPJ])

        if tamanho < 14:
            df.loc[i, consolidacao.CONTRATADO_CNPJ] = '0' * (14 - tamanho) + df.loc[i, consolidacao.CONTRATADO_CNPJ]

    return df

    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

    return df


def consolidar_pt_RO(data_extracao):
    dicionario_dados = {consolidacao.ANO: 'Exercicio', consolidacao.DOCUMENTO_DATA: 'DataDocumento',
                        consolidacao.UG_COD: 'UG', consolidacao.PROGRAMA_COD: 'CodPrograma',
                        consolidacao.PROGRAMA_DESCRICAO: 'Programa', consolidacao.ACAO_COD: 'CodAco',
                        consolidacao.ACAO_DESCRICAO: 'Acao', consolidacao.FUNCAO_COD: 'CodFuncao',
                        consolidacao.FUNCAO_DESCRICAO: 'NomFuncao', consolidacao.SUBFUNCAO_COD: 'CodSubFuncao',
                        consolidacao.DESPESA_DESCRICAO: 'NomDespesa', consolidacao.FONTE_RECURSOS_COD: 'Fonte',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'DesFonteRecurso',
                        consolidacao.CONTRATANTE_DESCRICAO: 'NomOrgao', consolidacao.UG_DESCRICAO: 'NomOrgao',
                        consolidacao.DOCUMENTO_NUMERO: 'documento', consolidacao.CONTRATADO_CNPJ: 'DocCredor',
                        consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                        consolidacao.SUBFUNCAO_DESCRICAO: 'DescricaoSubfuncao',
                        consolidacao.VALOR_EMPENHADO: 'ValorEmpenhada', consolidacao.VALOR_LIQUIDADO: 'ValorLiquidada',
                        consolidacao.VALOR_PAGO: 'ValorPaga', consolidacao.ORGAO_COD: 'CodOrgao'}
    planilha_original = path.join(config.diretorio_dados, 'RO', 'portal_transparencia', 'Despesas.CSV')
    df_original = pd.read_csv(planilha_original, sep=';', header=0, encoding='utf_16_le')
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RO
    df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'RO', '', data_extracao, pos_consolidar_pt_RO)
    return df


def consolidar_pt_PortoVelho(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ',
                        consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                        consolidacao.VALOR_EMPENHADO: 'Empenhado',
                        consolidacao.VALOR_LIQUIDADO: 'Liquidado',
                        consolidacao.VALOR_PAGO: 'Pago'}

    # Lê o arquivo "csv" de despesas baixado como um objeto pandas DataFrame
    df_original = pd.read_csv(path.join(config.diretorio_dados, 'RO', 'portal_transparencia',
                                        'PortoVelho', 'Download.csv'),
                              sep=';',
                              encoding='iso-8859-1')
    # Desconsidera a última coluna (não nomeada e sem dados) do objeto pandas DataFrame "df_original"
    df = df_original[['CPF/CNPJ', 'Credor', 'Empenhado', 'Anulado', 'Liquidado', 'Pago']]

    # Renomeia a coluna especificada do objeto pandas DataFrame "df"
    df.rename(columns={'Anulado': 'Valor Anulado'}, inplace=True)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_PortoVelho, 'RO',
                           get_codigo_municipio_por_nome('Porto Velho', 'RO'), data_extracao)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Rondônia')

    consolidacoes = consolidar_pt_RO(data_extracao)
    consolidacao_pt_PortoVelho = consolidar_pt_PortoVelho(data_extracao)

    consolidacoes = consolidacoes.append(consolidacao_pt_PortoVelho, ignore_index=True, sort=False)

    salvar(consolidacoes, 'RO')
