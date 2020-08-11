import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pre_processar_pt_Natal(df):
    # Reordena as colunas do objeto pandas DataFrame "df"
    df = df[['Credor', 'CPF/CNPJ', 'Empenhado', 'Anulado', 'Liquidado', 'Pago']]

    # Preenche com zeros à esquerda a coluna especificada convertida em string até ter...
    # tamanho 14
    df['CPF/CNPJ'] = df['CPF/CNPJ'].apply(lambda x: str(x).zfill(14))

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
    df_original = pd.read_excel(
        path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'Natal', 'Natal Transparente.xlsx'),
        skiprows=[0])

    # Chama a função "pre_processar_pt_Natal" definida neste módulo
    df = pre_processar_pt_Natal(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Natal, 'RN',
                           get_codigo_municipio_por_nome('Natal', 'RN'), data_extracao, pos_processar_pt)

    return df

def pos_processar_pt(df):
    for i in range(len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) >= 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df

def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Rio Grande do Norte')

    consolidacao_pt_Natal = consolidar_pt_Natal(data_extracao)
    df_consolidado = df_consolidado.append(consolidacao_pt_Natal)

    salvar(df_consolidado, 'RN')
