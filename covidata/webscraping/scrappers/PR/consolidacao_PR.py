import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar











def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Paraná')

    aquisicoes_capital = __consolidar_aquisicoes_capital(data_extracao)
    df_consolidado = df_consolidado.append(aquisicoes_capital)

    licitacoes_capital = __consolidar_licitacoes_capital(data_extracao)
    df_consolidado = df_consolidado.append(licitacoes_capital)

    salvar(df_consolidado, 'PR')
