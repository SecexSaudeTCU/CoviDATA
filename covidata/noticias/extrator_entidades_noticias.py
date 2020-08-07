import os

import logging
import time
import pandas as pd
from covidata.noticias.gnews import executar_busca

from covidata.noticias.parse_news import recuperar_textos


def get_NERs():
    from covidata.noticias.ner.spacy.spacy_ner import SpacyNER
    from covidata.noticias.ner.bert.bert_ner import BaseBERT_NER, Neuralmind_PT_BaseBERT_NER, \
        Neuralmind_PT_LargeBERT_NER
    from covidata.noticias.ner.polyglot.polyglot_ner import PolyglotNER
    return [#SpacyNER(),
            PolyglotNER(),
            #BaseBERT_NER(),
            #Neuralmind_PT_BaseBERT_NER(),
            #Neuralmind_PT_LargeBERT_NER()
     ]


if __name__ == '__main__':
    logger = logging.getLogger('covidata')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    start_time = time.time()
    logger.info('Buscando as notícias na Internet...')
    arquivo_noticias = executar_busca()
    df = recuperar_textos(arquivo_noticias)
    logger.info("--- %s minutos ---" % ((time.time() - start_time) / 60))

    start_time = time.time()
    logger.info('Extraindo entidades relevantes das notícias...')
    ners = get_NERs()

    writer = pd.ExcelWriter(os.path.join('ner.xlsx'), engine='xlsxwriter')

    for ner in ners:
        algoritmo = ner.__class__.__name__
        logger.info('Aplicando implementação ' + algoritmo)
        start_time = time.time()
        df_resultado = ner.extrair_entidades(df)
        logger.info("--- %s segundos ---" % (time.time() - start_time))
        df_resultado.to_excel(writer, sheet_name=algoritmo)

    writer.save()
    logger.info('Processamento concluído.')
