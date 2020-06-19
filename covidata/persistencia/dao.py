import os
from os import path

import config


def persistir(df, fonte, nome, uf):
    diretorio = path.join(config.diretorio_dados, uf, fonte)

    if not path.exists(diretorio):
        os.makedirs(diretorio)

    df.to_excel(path.join(diretorio, nome + '.xlsx'))
