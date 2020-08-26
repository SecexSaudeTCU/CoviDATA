import logging
import os
import time
from os import path

import pandas as pd

from covidata import config
from covidata.noticias.gnews import executar_busca
from covidata.noticias.ner.bert.bert_ner import BaseBERT_NER
from covidata.noticias.ner.polyglot.polyglot_ner import PolyglotNER
from covidata.noticias.parse_news import recuperar_textos


def get_NERs():
    from covidata.noticias.ner.spacy.spacy_ner import SpacyNER

    return [
        #SpacyNER(arquivo_validacao=path.join(config.diretorio_noticias, 'json_val.jsonl'), modo_avaliacao=True),
        #SpacyNER(diretorio_modelo_treinado=os.path.join(config.diretorio_raiz_modelos, 'spacy_PUB'),
         #            labels_validos=['PESSOA', 'LOC', 'ORG', 'PUB'], nome_algoritmo='FineTunedSpacyPUB',
         #            arquivo_validacao=path.join(config.diretorio_noticias, 'json_val.jsonl'), modo_avaliacao=True),
             PolyglotNER(),
             BaseBERT_NER()
            ]


def extrair_entidades(arquivo):
    logger = logging.getLogger('covidata')
    df = pd.read_excel(arquivo)

    logger.info('Extraindo entidades relevantes das notícias...')
    ners = get_NERs()
    writer = pd.ExcelWriter(os.path.join('ner.xlsx'), engine='xlsxwriter')
    for ner in ners:
        algoritmo = ner.get_nome_algoritmo()
        logger.info('Aplicando implementação ' + algoritmo)
        start_time = time.time()
        df_resultado = ner.extrair_entidades(df)
        #print(ner.avaliar())
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

    extrair_entidades(os.path.join(config.diretorio_noticias, 'com_textos_baseline_val.xlsx'))
