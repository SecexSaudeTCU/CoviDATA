import logging
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def consolidar_contratos():
    dicionario_dados = {consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.DESPESA_DESCRICAO: 'OBJETO', consolidacao.VALOR_CONTRATO: 'VALOR'}
    colunas_adicionais = ['Link para o contrato', 'Nº PROCESSO', 'Nº DO CONTRATO', 'PRAZO']
    planilha_original = path.join(config.diretorio_dados, 'BA', 'portal_transparencia', 'contratos.xls')
    df_original = pd.read_excel(planilha_original, header=4)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_BA
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'BA', '',None)
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
    return df


def consolidar():
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Bahia')
    contratos = consolidar_contratos()

    salvar(contratos, 'BA')
