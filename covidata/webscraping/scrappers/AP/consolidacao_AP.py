import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar






def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Amapá')

    contratacoes_capital = consolidar_contratacoes_capital(data_extracao)
    contratacoes = df_consolidado.append(contratacoes_capital)

    salvar(contratacoes, 'AP')

# consolidar(datetime.datetime.now())
