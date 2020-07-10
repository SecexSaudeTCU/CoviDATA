import logging
from os import path
from glob import glob

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar



def pre_processar_pt_Natal(df):

    # Reordena as colunas do objeto pandas DataFrame "df"
    df = df[['Credor', 'CPF/CNPJ', 'Empenhado', 'Anulado', 'Liquidado', 'Pago']]

    df['CPF/CNPJ'] = df['CPF/CNPJ'].apply(lambda x: str(x).zfill(14))

    return df


def pos_processar_pt(df):

    for i in range(len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) >= 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df


def consolidar_pt_RN(data_extracao):

    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Contratante',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto',
                        consolidacao.CONTRATADO_DESCRICAO: 'Contratado (a)',
                        consolidacao.DOCUMENTO_NUMERO: 'N. Contrato',
                        consolidacao.VALOR_CONTRATO: 'Valor do Contrato (R$)',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'Fonte do Recurso',
                        consolidacao.VALOR_PAGO: 'Valor Pago (R$)'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['N. Processo', 'Modalidade', 'Fundamento Legal', 'Data Assinatura',
                          'Vigência', 'Valor anulado (R$)']

    # Obtém objeto list dos arquivos armazenados no path passado como argumento para a função nativa "glob"
    list_files = glob(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte/*'))

    # Obtém o primeiro elemento do objeto list que corresponde ao nome do arquivo "csv" baixado
    file_name = list_files[0]

    # Lê o arquivo "csv" de nome "file_name" de contratos baixado como um objeto pandas DataFrame
    # Usa o parâmetro "error_bad_lines" como "False" para ignorar linhas com problema do arquivo "csv" (primeira solução)
    df_original = pd.read_csv(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte', file_name),
                              sep=';', error_bad_lines=False)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RN, 'RN', '',
                           data_extracao, pos_processar_pt)
    return df


def consolidar_pt_Natal(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                        consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ',
                        consolidacao.VALOR_EMPENHADO: 'Empenhado',
                        consolidacao.VALOR_LIQUIDADO: 'Liquidado',
                        consolidacao.VALOR_PAGO: 'Pago'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Anulado']

    # Lê o arquivo "xlsx" de nome de despesas baixado como um objeto pandas DataFrame
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'Natal', 'Natal Transparente.xlsx'),
                                skiprows=[0])

    # Chama a função "pre_processar_pt_Natal" definida neste módulo
    df = pre_processar_pt_Natal(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Natal, 'RN',
                           get_codigo_municipio_por_nome('Natal', 'RN'), data_extracao, pos_processar_pt)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Rio Grande do Norte')

    consolidacoes = consolidar_pt_RN(data_extracao)
    consolidacao_pt_Natal = consolidar_pt_Natal(data_extracao)

    consolidacoes = consolidacoes.append(consolidacao_pt_Natal, ignore_index=True, sort=False)

    salvar(consolidacoes, 'RN')
