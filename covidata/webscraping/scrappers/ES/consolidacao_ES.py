import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_dados_contratos_emergenciais(df):
    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if 'EX' not in cpf_cnpj:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


def pos_processar_licitacoes_capital(df):
    df[consolidacao.ANO] = consolidacao.ANO_PADRAO
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Vitória'
    return df


def __consolidar_dados_contratos_emergenciais(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão Contratante',
                        consolidacao.UG_DESCRICAO: 'Órgão Contratante',
                        consolidacao.CONTRATADO_DESCRICAO: 'Nome do Contratado',
                        consolidacao.CONTRATADO_CNPJ: 'CPF / CNPJ do Contratado',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'Valor Total',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor Unitário',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Quantidade',
                        consolidacao.ITEM_EMPENHO_UNIDADE_MEDIDA: 'Unidade',
                        consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade de Licitação',
                        consolidacao.DATA_ASSINATURA: 'Data de assinatura',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local de Entrega / Execução',
                        consolidacao.NUMERO_CONTRATO: 'Número / Ano do instrumento contratual',
                        consolidacao.NUMERO_PROCESSO: 'Número do Processo de Contratação / aquisição'}
    colunas_adicionais = ['Prazo de vigência', 'Link do Processo de Contratação/Aquisição',
                          'Termo de Referência / Projeto Básico', 'Íntegra do Instrumento Contratual']
    planilha_original = path.join(config.diretorio_dados, 'ES', 'portal_transparencia',
                                  'dados-contratos-emergenciais-covid-19.csv')
    df_original = pd.read_csv(planilha_original, encoding="ISO-8859-1", sep=';')
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_ES
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'ES', '', data_extracao, pos_processar_dados_contratos_emergenciais)

    return df


def __consolidar_licitacoes_capital(data_extracao):
    dicionario_dados = {consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade', consolidacao.DESPESA_DESCRICAO: 'Objeto'}
    colunas_adicionais = ['Número/Ano', 'Processo/Ano', 'Data Ratificação', 'Enquadramento']
    planilha_original = path.join(config.diretorio_dados, 'ES', 'portal_transparencia', 'Vitoria',
                                  'TransparenciaWeb.Licitacoes.Lista.xlsx')
    df_original = pd.read_excel(planilha_original, header=3)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Vitoria
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'ES', get_codigo_municipio_por_nome('Vitória', 'ES'), data_extracao,
                           pos_processar_licitacoes_capital)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Espírito Santo')

    dados_contratos_emergenciais = __consolidar_dados_contratos_emergenciais(data_extracao)
    licitacoes_capital = __consolidar_licitacoes_capital(data_extracao)
    dados_contratos_emergenciais = dados_contratos_emergenciais.append(licitacoes_capital)

    salvar(dados_contratos_emergenciais, 'ES')
