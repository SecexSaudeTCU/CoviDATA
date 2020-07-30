import datetime
import logging
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_contratacoes(df):
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
    return df


def pos_processar_contratacoes_capital(df):
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Macapá'

    df['temp'] = df[consolidacao.CONTRATANTE_DESCRICAO]
    df[consolidacao.CONTRATANTE_DESCRICAO] = df.apply(lambda row: row['temp'][0:row['temp'].find('-')], axis=1)
    df[consolidacao.LOCAL_EXECUCAO_OU_ENTREGA] = df.apply(
        lambda row: row['temp'][row['temp'].find('-') + len(' LOCAL DE EXECUÇÃO: '):len(row['temp'])], axis=1)
    df[consolidacao.UG_DESCRICAO] = df[consolidacao.CONTRATANTE_DESCRICAO]
    df = df.drop(['temp'], axis=1)

    df['temp'] = df[consolidacao.CONTRATADO_CNPJ]
    df[consolidacao.CONTRATADO_DESCRICAO] = df.apply(lambda row: row['temp'][0:row['temp'].find('/')], axis=1)
    df[consolidacao.CONTRATADO_CNPJ] = df.apply(lambda row: row['temp'][row['temp'].find('/') + 1:len(row['temp'])],
                                                axis=1)
    df = df.drop(['temp'], axis=1)

    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ].strip()

        if len(cpf_cnpj) == 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        prazo_contratual = df.loc[i, 'PRAZO CONTRATUAL']
        data_inicio = prazo_contratual[0:prazo_contratual.find(' a ')]
        data_fim = prazo_contratual[prazo_contratual.find(' a ') + 3:len(prazo_contratual)]
        df.loc[i, consolidacao.DATA_INICIO_VIGENCIA] = data_inicio
        df.loc[i, consolidacao.DATA_FIM_VIGENCIA] = data_fim

    df = df.drop(['PRAZO CONTRATUAL'], axis=1)

    return df


def consolidar_contratacoes(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'orgao', consolidacao.UG_DESCRICAO: 'orgao',
                        consolidacao.MOD_APLIC_DESCRICAO: 'modalidade_processo',
                        consolidacao.DESPESA_DESCRICAO: 'objeto', consolidacao.CONTRATADO_CNPJ: 'fornecedor_cnpj_cpf',
                        consolidacao.CONTRATADO_DESCRICAO: 'fornecedor_razao_social',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'quantidade',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'valor_unitario',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'valor_total',
                        consolidacao.VALOR_CONTRATO: 'valor_total',
                        consolidacao.FONTE_RECURSOS_COD: 'fontes_de_recursos_fr_codigo',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'fontes_de_recursos_fr_desc',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'local',
                        consolidacao.DATA_ASSINATURA: 'data_assinatura', consolidacao.NUMERO_PROCESSO: 'processo'}
    colunas_adicionais = ['id', 'numero_siga', 'duracao', 'anexo_id']
    planilha_original = path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'contratacoes.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AP
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'AP', '', data_extracao, pos_processar_contratacoes)

    return df


def consolidar_contratacoes_capital(data_extracao):
    dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'Contratado - CNPJ / CPF',
                        consolidacao.DESPESA_DESCRICAO: 'Descrição de bem ou serviço',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Quantidade',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor Unitário',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'Valor Total',
                        consolidacao.CONTRATANTE_DESCRICAO: 'Órgão Contratante',
                        consolidacao.UG_DESCRICAO: 'Órgão Contratante',
                        consolidacao.VALOR_CONTRATO: 'Valor contratado', consolidacao.VALOR_PAGO: 'Valor pago',
                        consolidacao.MOD_APLIC_DESCRICAO: 'Forma / modalidade',
                        consolidacao.DATA_CELEBRACAO: 'Data de Celebração / Publicação',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local de execução'}
    colunas_adicionais = ['Nº e Íntegra do Processo / COntrato', 'Prazo Contratual']
    planilha_original = path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa', 'transparencia.xlsx')
    df_original = pd.read_excel(planilha_original, header=1)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Macapa
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'AP', get_codigo_municipio_por_nome('Macapá', 'AP'), data_extracao,
                           pos_processar_contratacoes_capital)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Amapá')
    contratacoes = consolidar_contratacoes(data_extracao)

    contratacoes_capital = consolidar_contratacoes_capital(data_extracao)
    contratacoes = contratacoes.append(contratacoes_capital)

    salvar(contratacoes, 'AP')


#consolidar(datetime.datetime.now())
