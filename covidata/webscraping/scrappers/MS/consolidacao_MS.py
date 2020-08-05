import datetime
import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_despesas_capital(df):
    df = df.rename(columns={'PROCESSO DE ORIGEM': 'NÚMERO PROCESSO'})
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Campo Grande'
    return df


def __consolidar_compras_emergenciais(data_extracao):
    dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ', consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                        consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.DESPESA_DESCRICAO: 'Objeto',
                        consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade', consolidacao.VALOR_CONTRATO: 'Valor'}
    colunas_adicionais = ['Número Processo']
    planilha_original = path.join(config.diretorio_dados, 'MS', 'portal_transparencia',
                                  'ComprasEmergenciaisMS_COVID19.csv')
    df_original = pd.read_csv(planilha_original, sep=';', header=2, index_col=False)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MS
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'MS', '', data_extracao)
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
    return df


def __consolidar_despesas_capital(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.UG_DESCRICAO: 'Unidade',
                        consolidacao.CONTRATADO_DESCRICAO: 'Fornecedor',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'Elemento Despesa',
                        consolidacao.VALOR_EMPENHADO: 'Total Empenhado',
                        consolidacao.VALOR_LIQUIDADO: 'Total Liquidado', consolidacao.VALOR_PAGO: 'Total Pago',
                        consolidacao.CATEGORIA_ECONOMICA_DESCRICAO: 'Categoria'}
    colunas_adicionais = ['Processo de Origem', 'Data', 'Status']
    planilha_original = path.join(config.diretorio_dados, 'MS', 'portal_transparencia', 'Campo Grande',
                                  'Despesas – Transparência Covid19 – Prefeitura de Campo Grande.xlsx')
    df_original = pd.read_excel(planilha_original, header=1)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_CampoGrande
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'MS', get_codigo_municipio_por_nome('Campo Grande', 'MS'), data_extracao,
                           pos_processar_despesas_capital)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Mato Grosso do Sul')

    compras_emergenciais = __consolidar_compras_emergenciais(data_extracao)
    despesas_capital = __consolidar_despesas_capital(data_extracao)
    compras_emergenciais = compras_emergenciais.append(despesas_capital)

    salvar(compras_emergenciais, 'MS')
    #salvar(despesas_capital, 'MS')

#consolidar(datetime.datetime.now())