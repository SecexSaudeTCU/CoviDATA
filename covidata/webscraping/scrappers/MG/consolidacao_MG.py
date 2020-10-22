import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_contratacoes_capital(df):
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Belo Horizonte'
    df = df.rename(columns={'PROCESSO_COMPRA': 'NÚMERO DO PROCESSO DE COMPRA'})
    return df


def __consolidar_compras(data_extracao):
    dicionario_dados = {consolidacao.ORGAO_COD: 'Código Órgão Demandante ',
                        consolidacao.CONTRATANTE_DESCRICAO: 'Órgão Demandante ',
                        consolidacao.UG_DESCRICAO: 'Órgão Demandante ',
                        consolidacao.MOD_APLIC_DESCRICAO: 'Procedimento de Contratação ',
                        consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ do Contratado ',
                        consolidacao.CONTRATADO_DESCRICAO: 'Contratado ',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto do Processo ',
                        consolidacao.VALOR_CONTRATO: 'Valor Homologado ',
                        consolidacao.DATA_PUBLICACAO: 'Data da Publicação ',
                        consolidacao.DATA_FIM_VIGENCIA: 'Fim da Vigência ',
                        consolidacao.DATA_INICIO_VIGENCIA: 'Início da Vigência ',
                        consolidacao.NUMERO_CONTRATO: 'Número do Contrato '}
    planilha_original = path.join(config.diretorio_dados, 'MG', 'portal_transparencia',
                                  '_Compras - Programa de enfrentamento COVID-19.csv')
    df_original = pd.read_csv(planilha_original, sep=';')
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MG
    df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'MG', '', data_extracao)
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
    return df


def __consolidar_contratacoes_capital(data_extracao):
    dicionario_dados = {consolidacao.ANO: 'ANO', consolidacao.CONTRATANTE_DESCRICAO: 'ORGAO_ENTIDADE',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ_CPF_CONTRATADO',
                        consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO', consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'QUANTIDADE',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'VALOR_UNITÁRIO',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'VALOR_TOTAL',
                        consolidacao.MOD_APLIC_DESCRICAO: 'MODALIDADE',
                        consolidacao.DATA_INICIO_VIGENCIA: 'DATA_INICIO_VIGENCIA',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'LOCAL_EXECUCAO',
                        consolidacao.DATA_FIM_VIGENCIA: 'DATA_FIM_VIGENCIA'}
    planilha_original = path.join(config.diretorio_dados, 'MG', 'portal_transparencia', 'Belo Horizonte',
                                  'contratacaocorona.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_BeloHorizonte
    df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'MG', get_codigo_municipio_por_nome('Belo Horizonte', 'MG'), data_extracao,
                           pos_processar_contratacoes_capital)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Minas Gerais')

    compras = __consolidar_compras(data_extracao)
    contratacoes_capital = __consolidar_contratacoes_capital(data_extracao)
    compras = compras.append(contratacoes_capital)

    salvar(compras, 'MG')
