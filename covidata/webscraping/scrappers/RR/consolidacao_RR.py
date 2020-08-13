import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_pt_BoaVista(df):
    for i in range(len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(str(cpf_cnpj)) >= 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Boa Vista'
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


def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Roraima')

    consolidacao_pt_BoaVista = consolidar_pt_BoaVista(data_extracao)

    df_consolidado = df_consolidado.append(consolidacao_pt_BoaVista, ignore_index=True, sort=False)

    salvar(df_consolidado, 'RR')
