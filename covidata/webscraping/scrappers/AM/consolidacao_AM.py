import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar









def consolidar(data_extracao):
    logger = logging.getLogger('covidata')



    salvar(contratos, 'AM')
