import logging
import time

from covidata.noticias.gnews import executar_busca

from covidata.noticias.parse_news import recuperar_textos


def get_NER():
    from covidata.noticias.ner.spacy.spacy_ner import SpacyNER
    return SpacyNER()

    #from covidata.noticias.ner.bert.bert_ner import BaseBERT_NER
    #return BaseBERT_NER()


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
    ner = get_NER()
    diretorio_saida = ner.extrair_entidades(df)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
    logger.info('Processamento concluído.  Resultado salvo em: ' + diretorio_saida)
