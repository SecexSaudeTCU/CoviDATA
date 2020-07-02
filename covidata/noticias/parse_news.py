"""
Work in progress...

TODO:

"""
import pandas as pd
import time
from newspaper import Article
import pt_core_news_sm # baixado com o spaCy


def get_text(url, max_retries=5, sleep=5):
    """
    Extrai e retorna texto do artigo da URL
    """

    # Tentar max_retries vezes
    for i in range(0, max_retries):
        try:
            article = Article(row.link)
            article.download()
            article.parse()
        except:
            print(f'Erro ao processar {url}. Tentando novamente em {sleep} segundos ({i+1}/{max_retries})')
            time.sleep(sleep)
        return article.text

    print(f'Número máximo de tentativas atingido. Retornando ERRO')
    return 'ERRO'


if __name__ == '__main__':

    #
    df = pd.read_excel('covidata/noticias/noticias_n_202.xlsx')


    textos = []
    sleep_time = 5

    for i, row in df.iterrows():
        print(f'Processando URL {i}')
        texto = get_text(row.link)
        textos.append(texto)

    df['texto'] = textos
    df.to_excel('com_textos.xlsx')


    """
    nlp = pt_core_news_sm.load()
    doc = nlp(texto)
    from spacy import displacy
    displacy.serve(doc, style="ent")

    for ent in doc.ents:
        print(ent.label_, ent)
    """
