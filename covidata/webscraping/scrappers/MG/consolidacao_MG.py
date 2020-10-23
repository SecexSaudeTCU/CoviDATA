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


def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Minas Gerais')

    contratacoes_capital = __consolidar_contratacoes_capital(data_extracao)
    df_consolidado = df_consolidado.append(contratacoes_capital)

    salvar(df_consolidado, 'MG')
