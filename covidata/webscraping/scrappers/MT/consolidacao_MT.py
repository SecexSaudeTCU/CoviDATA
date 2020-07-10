import logging
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_consolidar_contratos(df):
    df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    for i in range(0, len(df)):
        tamanho = len(df.loc[i, consolidacao.CONTRATADO_CNPJ])

        if tamanho > 2:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

            if tamanho < 14:
                df.loc[i, consolidacao.CONTRATADO_CNPJ] = '0' * (14 - tamanho) + df.loc[i, consolidacao.CONTRATADO_CNPJ]

    return df


def __consolidar_contratos(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Entidade', consolidacao.UG_DESCRICAO: 'Entidade',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.VALOR_CONTRATO: 'Valor',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ', consolidacao.CONTRATADO_DESCRICAO: 'Razão Social'}
    colunas_adicionais = ['Link para o contrato', 'Contrato', 'Situação', 'Tipo de Contrato', 'Data de Vigência']
    planilha_original = path.join(config.diretorio_dados, 'MT', 'portal_transparencia', 'Contratos.xls')
    df_original = pd.read_excel(planilha_original, header=4)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MT
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'MT', '', data_extracao, pos_consolidar_contratos)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Mato Grosso')
    contratos = __consolidar_contratos(data_extracao)
    salvar(contratos, 'MT')

