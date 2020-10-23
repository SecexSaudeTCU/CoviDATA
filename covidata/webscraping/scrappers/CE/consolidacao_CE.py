from os import path

import logging
import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import salvar, consolidar_layout


def __consolidar_gastos(data_extracao):
    dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: 'numero_empenho', consolidacao.DOCUMENTO_DATA: 'data_empenho',
                        consolidacao.CONTRATANTE_DESCRICAO: 'orgao', consolidacao.UG_DESCRICAO: 'orgao',
                        consolidacao.VALOR_EMPENHADO: 'valor_empenho', consolidacao.CONTRATADO_DESCRICAO: 'credor',
                        consolidacao.MOD_APLIC_DESCRICAO: 'modalidade_licitacao',
                        consolidacao.ITEM_EMPENHO_DESCRICAO: 'item', consolidacao.VALOR_CONTRATO: 'valor_empenho',
                        consolidacao.FUNDAMENTO_LEGAL: 'fund_legal', consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'local',
                        consolidacao.NUMERO_CONTRATO: 'numero_contrato',
                        consolidacao.NUMERO_PROCESSO: 'numero_processo', consolidacao.DATA_FIM_VIGENCIA: 'data_termino',
                        consolidacao.LINK_CONTRATO: 'integra_contrato'}
    planilha_original = path.join(config.diretorio_dados, 'CE', 'portal_transparencia',
                                  'gasto_covid_dados_abertos.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_CE
    df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'CE', '', data_extracao)
    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
    return df


def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Ceará')
    gastos = __consolidar_gastos(data_extracao)

    gastos = gastos.append(df_consolidado)

    salvar(gastos, 'CE')
