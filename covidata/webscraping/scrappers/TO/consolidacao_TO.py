import datetime
import logging

from covidata import config
from covidata.persistencia import consolidacao
from os import path
import pandas as pd

from covidata.persistencia.consolidacao import consolidar_layout, salvar


def __consolidar_contratos(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ',
                        consolidacao.CONTRATADO_DESCRICAO: 'Contratado', consolidacao.UG_DESCRICAO: 'Órgão',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.ITEM_EMPENHO_UNIDADE_MEDIDA: 'Unidade',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Qtde', consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'Total'}
    colunas_adicionais = ['Processo', 'Tipo de Contratação', 'Contrato', 'Data', 'Prazo (dias)', 'Local de Entrega']
    planilha_original = path.join(config.diretorio_dados, 'TO', 'portal_transparencia', 'contratos.xls')
    df_original = pd.read_excel(planilha_original, header=4)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_TO
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'TO', '', data_extracao)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Tocantins')

    contratos = __consolidar_contratos(data_extracao)

    salvar(contratos, 'TO')

consolidar(datetime.datetime.now())