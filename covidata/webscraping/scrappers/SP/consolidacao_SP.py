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


def pos_processar_PT(df):
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) == 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) == 18:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


def consolidar_PT(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Secretaria', consolidacao.UG_DESCRICAO: 'Secretaria',
                        consolidacao.NUMERO_PROCESSO: 'Número do Processo',
                        consolidacao.CONTRATADO_DESCRICAO: 'Contratada / Conveniada',
                        consolidacao.CONTRATADO_CNPJ: 'CPF / CNPJ / CGC',
                        consolidacao.DESPESA_DESCRICAO: 'Descrição Processo',
                        consolidacao.ITEM_EMPENHO_DESCRICAO: 'Finalidade/Item',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Quantidade',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor Unitário',
                        consolidacao.DOCUMENTO_NUMERO: 'Nota de Empenho', consolidacao.VALOR_EMPENHADO: 'Empenho',
                        consolidacao.FONTE_RECURSOS_COD: 'Código Fonte Recurso',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'Código Nome Fonte Detalhada',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local Entrega'}
    colunas_adicionais = ['Modalidade de Contratação', 'Data da Movimentação', 'Tipo de Pagamento',
                          'Número de Pagamento', 'Valor NE', 'Valor NL', 'Valor NP', 'Valor OB']
    df_original = pd.read_csv(path.join(config.diretorio_dados, 'SP', 'portal_transparencia', 'COVID.csv'),
                              encoding='ISO-8859-1', sep=';')
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SP, 'SP',
                           None, data_extracao, pos_processar_PT)
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

    consolidacoes = consolidar_PT(data_extracao)

    consolidacoes = consolidacoes.append(df_consolidado)

    # TODO: Indisponível/instável
    # consolidacao_tcm = consolidar_tcm(data_extracao)
    # consolidacoes = consolidacoes.append(consolidacao_tcm, ignore_index=True, sort=False)

    salvar(consolidacoes, 'SP')
    # salvar(consolidacoes_capital, 'SP')

# consolidar(datetime.datetime.now())
