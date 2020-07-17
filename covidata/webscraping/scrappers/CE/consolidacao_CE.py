import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import salvar, consolidar_layout


def pos_processar_despesas_capital(df):
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Fortaleza'

    df['temp'] = df[consolidacao.CONTRATANTE_DESCRICAO]
    df[consolidacao.CONTRATANTE_DESCRICAO] = df.apply(
        lambda row: row['temp'][row['temp'].find('- ') + 1:len(row['temp'])], axis=1)
    df[consolidacao.UG_COD] = df.apply(lambda row: row['temp'][0:row['temp'].find('-')], axis=1)
    df[consolidacao.UG_DESCRICAO] = df[consolidacao.CONTRATANTE_DESCRICAO]
    df = df.drop(['temp'], axis=1)

    for i in range(0, len(df)):
        __definir_tipo_contratado(df, i)
        __definir_situacao_empenho(df, i)

    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

    return df


def __definir_situacao_empenho(df, i):
    situacao_empenho = df.loc[i, 'SITUACAO EMPENHO']
    if situacao_empenho == 'Liquidado':
        df.loc[i, consolidacao.VALOR_LIQUIDADO] = df.loc[i, consolidacao.VALOR_EMPENHADO]
    elif situacao_empenho == 'Pago':
        df.loc[i, consolidacao.VALOR_LIQUIDADO] = df.loc[i, consolidacao.VALOR_EMPENHADO]
        df.loc[i, consolidacao.VALOR_PAGO] = df.loc[i, consolidacao.VALOR_EMPENHADO]


def __definir_tipo_contratado(df, i):
    cpf_cnpj = str(df.loc[i, consolidacao.CONTRATADO_CNPJ])
    if len(cpf_cnpj) == 11:
        df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
    elif len(cpf_cnpj) > 11:
        df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ


def __consolidar_gastos(data_extracao):
    dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: 'numero_empenho', consolidacao.DOCUMENTO_DATA: 'data_empenho',
                        consolidacao.CONTRATANTE_DESCRICAO: 'orgao', consolidacao.UG_DESCRICAO: 'orgao',
                        consolidacao.VALOR_EMPENHADO: 'valor_empenho', consolidacao.CONTRATADO_DESCRICAO: 'credor',
                        consolidacao.MOD_APLIC_DESCRICAO: 'modalidade_licitacao',
                        consolidacao.ITEM_EMPENHO_DESCRICAO: 'item', consolidacao.VALOR_CONTRATO: 'valor_empenho',
                        consolidacao.FUNDAMENTO_LEGAL: 'fund_legal', consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'local',
                        consolidacao.NUMERO_CONTRATO: 'numero_contrato',
                        consolidacao.NUMERO_PROCESSO: 'numero_processo', consolidacao.DATA_FIM_VIGENCIA: 'data_termino',
                        consolidacao.LINK_CONTRATO:'integra_contrato'}
    colunas_adicionais = ['natureza', 'num_certidao', 'processo_licitacao']
    planilha_original = path.join(config.diretorio_dados, 'CE', 'portal_transparencia',
                                  'gasto_covid_dados_abertos.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_CE
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'CE', '', data_extracao)
    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
    return df


def __consolidar_despesas_capital(data_extracao):
    dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: 'N. DO EMPENHO', consolidacao.DOCUMENTO_DATA: 'DATA EMPENHO',
                        consolidacao.CONTRATANTE_DESCRICAO: 'UNIDADE ORCAMENTARIA',
                        consolidacao.UG_DESCRICAO: 'UNIDADE ORCAMENTARIA',
                        consolidacao.VALOR_EMPENHADO: 'VALOR EMPENHO', consolidacao.VALOR_CONTRATO: 'VALOR EMPENHO',
                        consolidacao.CONTRATADO_DESCRICAO: 'CREDOR', consolidacao.CONTRATADO_CNPJ: 'CNPJ / CPF',
                        consolidacao.NUMERO_CONTRATO: 'N. CONTRATO',
                        consolidacao.NUMERO_PROCESSO: 'N. PROCESSO DE AQUISICAO'}
    colunas_adicionais = ['TIPO DESPESA', 'SITUACAO EMPENHO', 'VALOR ANULADO']
    planilha_original = path.join(config.diretorio_dados, 'CE', 'portal_transparencia', 'Fortaleza', 'despesas.csv')
    df_original = pd.read_csv(planilha_original, sep=';')
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Fortaleza
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'CE', get_codigo_municipio_por_nome('Fortaleza', 'CE'), data_extracao,
                           pos_processar_despesas_capital)
    return df


def consolidar(data_extracao):
    # TODO: Unificar formatação de datas
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Ceará')
    gastos = __consolidar_gastos(data_extracao)

    despesas_capital = __consolidar_despesas_capital(data_extracao)
    gastos = gastos.append(despesas_capital)

    salvar(gastos, 'CE')
