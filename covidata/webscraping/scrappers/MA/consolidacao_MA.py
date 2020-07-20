import logging
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_municipios_por_uf, get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_licitacoes(df):
    df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    codigos_municipios = get_municipios_por_uf('MA')
    # Define os municípios
    df[consolidacao.COD_IBGE_MUNICIPIO] = df.apply(
        lambda row: codigos_municipios.get(row[consolidacao.MUNICIPIO_DESCRICAO].upper(), ''), axis=1)
    df = df.astype({consolidacao.COD_IBGE_MUNICIPIO: str})

    df.loc[(df[consolidacao.COD_IBGE_MUNICIPIO] != ''), consolidacao.ESFERA] = consolidacao.ESFERA_MUNICIPAL

    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) == 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        vigencia = df.loc[i, 'VIGÊNCIA']
        data_inicio = vigencia[0:vigencia.find(' à ')]
        data_fim = vigencia[vigencia.find(' à ') + 3:len(vigencia)]
        df.loc[i, consolidacao.DATA_INICIO_VIGENCIA] = data_inicio
        df.loc[i, consolidacao.DATA_FIM_VIGENCIA] = data_fim

    df = df.drop(['VIGÊNCIA'], axis=1)
    return df


def pos_processar_portal_transparencia_capital(df):
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'São Luís'
    df = df.rename(columns={'Nº DO PROCESSO': 'Nº PROCESSO'})

    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) == 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


def __consolidar_licitacoes(data_extracao):
    dicionario_dados = {consolidacao.MUNICIPIO_DESCRICAO: 'ENTE', consolidacao.CONTRATANTE_DESCRICAO: 'UNIDADE',
                        consolidacao.UG_DESCRICAO: 'UNIDADE', consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                        consolidacao.VALOR_CONTRATO: 'VALOR',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'ORIGEM DO RECURSO',
                        consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO', consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ',
                        consolidacao.DATA_ASSINATURA: 'DATA ASSINATURA', consolidacao.NUMERO_CONTRATO: 'Nº CONTRATO',
                        consolidacao.NUMERO_PROCESSO:'Nº PROCESSO'}
    colunas_adicionais = ['VIGÊNCIA']
    planilha_original = path.join(config.diretorio_dados, 'MA', 'tce', 'licitacoes.xls')
    df_original = pd.read_excel(planilha_original, header=1)
    fonte_dados = consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_MA
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'MA', '', data_extracao, pos_processar_licitacoes)
    return df


def __consolidar_portal_transparencia_estado(data_extracao):
    dicionario_dados = {consolidacao.CONTRATADO_DESCRICAO: 'Contratado', consolidacao.NUMERO_CONTRATO: 'contrato'}
    colunas_adicionais = ['Fonte contratado estadual', 'Fonte contratado federal', 'Fonte contratado doações',
                          'Fonte pago estadual', 'Fonte pago federal', 'Fonte pago doações']
    planilha_original = path.join(config.diretorio_dados, 'MA', 'portal_transparencia',
                                  'Portal da Transparência do Governo do Estado do Maranhão.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MA
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'MA', '', data_extracao)
    return df


def __consolidar_portal_transparencia_capital(data_extracao):
    dicionario_dados = {consolidacao.VALOR_CONTRATO: 'Valor do Contrato (R$)',
                        consolidacao.DESPESA_DESCRICAO: 'Descrição', consolidacao.CONTRATADO_DESCRICAO: 'Empresa',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ', consolidacao.CONTRATANTE_DESCRICAO: 'Unidade Contratante',
                        consolidacao.DATA_ASSINATURA: 'Data de Assinatura', consolidacao.LINK_CONTRATO: 'Link contrato',
                        consolidacao.NUMERO_CONTRATO: 'Nº Contrato'}
    colunas_adicionais = ['Nº do Processo', 'Destinação de Uso', 'Vigência']
    planilha_original = path.join(config.diretorio_dados, 'MA', 'portal_transparencia', 'São Luís', 'contratacoes.xls')
    df_original = pd.read_excel(planilha_original, header=4)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SaoLuis
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'MA', get_codigo_municipio_por_nome('São Luís', 'MA'), data_extracao,
                           pos_processar_portal_transparencia_capital)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Maranhão')
    #TODO: Planilha corrupta
    licitacoes = __consolidar_licitacoes(data_extracao)

    portal_transparencia_estado = __consolidar_portal_transparencia_estado(data_extracao)
    licitacoes = licitacoes.append(portal_transparencia_estado)

    portal_transparencia_capital = __consolidar_portal_transparencia_capital(data_extracao)
    licitacoes = licitacoes.append(portal_transparencia_capital)

    salvar(licitacoes, 'MA')
