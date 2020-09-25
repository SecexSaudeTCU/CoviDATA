import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar







def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Rio Grande do Norte')

    consolidacao_pt_Natal = consolidar_pt_Natal(data_extracao)
    df_consolidado = df_consolidado.append(consolidacao_pt_Natal)

    salvar(df_consolidado, 'RN')
