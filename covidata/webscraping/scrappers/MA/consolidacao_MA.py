import logging
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_municipios_por_uf, get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


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


def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Maranhão')

    portal_transparencia_estado = __consolidar_portal_transparencia_estado(data_extracao)
    df_consolidado = df_consolidado.append(portal_transparencia_estado)

    salvar(df_consolidado, 'MA')
