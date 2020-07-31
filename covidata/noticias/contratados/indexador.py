import os.path
import time

import pandas as pd
from whoosh import index
from whoosh.fields import Schema, TEXT

from covidata import config
from covidata.persistencia import consolidacao


def criar_indice():
    schema = Schema(contratado_descricao=TEXT(stored=True))

    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")

    ix = index.create_in("indexdir", schema)
    writer = ix.writer()
    print('Lendo arquivo com dados consolidados...')
    start_time = time.time()
    arquivo = os.path.join(config.diretorio_dados, 'consolidados', 'UFs.xlsx')
    df = pd.read_excel(arquivo)
    print("--- %s segundos ---" % (time.time() - start_time))
    contratados = df[consolidacao.CONTRATADO_DESCRICAO]
    print('Criando o índice...')
    start_time = time.time()

    for contratado in contratados:
        writer.add_document(contratado_descricao=str(contratado).strip())

    writer.commit()
    print('Índice criado.')
    print("--- %s segundos ---" % (time.time() - start_time))
