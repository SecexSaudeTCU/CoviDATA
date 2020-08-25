import logging
import os
import time

import pandas as pd

from covidata import config
from covidata.noticias.gnews import executar_busca
from covidata.noticias.parse_news import recuperar_textos
from os import path


def get_NERs():
    from covidata.noticias.ner.spacy.spacy_ner import SpacyNER
    from covidata.noticias.ner.bert.bert_ner import BaseBERT_NER
    from covidata.noticias.ner.polyglot.polyglot_ner import PolyglotNER

    return [SpacyNER(), SpacyNER(diretorio_modelo_treinado=os.path.join(config.diretorio_raiz_modelos, 'spacy_PUB'),
                                 labels_validos=['PESSOA', 'LOC', 'ORG', 'PUB'], nome_algoritmo='FineTunedSpacyPUB'),
            # PolyglotNER(True),
            # BaseBERT_NER()
            ]


def extrair_entidades():
    logger = logging.getLogger('covidata')
    df = pd.read_excel('com_textos_baseline.xlsx')

    logger.info('Extraindo entidades relevantes das notícias...')
    ners = get_NERs()
    writer = pd.ExcelWriter(os.path.join('ner.xlsx'), engine='xlsxwriter')
    for ner in ners:
        algoritmo = ner.get_nome_algoritmo()
        logger.info('Aplicando implementação ' + algoritmo)
        start_time = time.time()
        df_resultado = ner.extrair_entidades(df)
        print(ner.avaliar())
        logger.info("--- %s segundos ---" % (time.time() - start_time))
        df_resultado.to_excel(writer, sheet_name=algoritmo)
    writer.save()
    logger.info('Processamento concluído.')


def obter_textos():
    logger = logging.getLogger('covidata')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    start_time = time.time()
    logger.info('Buscando as notícias na Internet...')
    arquivo_noticias = executar_busca()
    recuperar_textos(arquivo_noticias)
    logger.info("--- %s minutos ---" % ((time.time() - start_time) / 60))


if __name__ == '__main__':
    # obter_textos()

    extrair_entidades()
