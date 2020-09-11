import logging
import os
import time
from collections import defaultdict

import pandas as pd

from covidata.cnpj.busca_util import processar_descricao_contratado
from covidata.cnpj.indice import buscar_por_razao_social
from covidata.cnpj.rfb import DAO_RFB
from covidata.noticias.gnews import executar_busca
from covidata.noticias.ner.bert.bert_ner import FinedTunedBERT_NER
from covidata.noticias.parse_news import recuperar_textos


def __get_NERs():
    return [
        FinedTunedBERT_NER()
    ]


def extrair_entidades(arquivo):
    """
    Extrai as entidade de um arquivo com extensão .xlsx que contém o conjunto de textos a serem analisados, bem como
    seus metadados.  O resultado é salvo em um arquivo chamado ner.xlsx.

    :param arquivo Caminho para o arquivo.
    """
    logger = logging.getLogger('covidata')
    df = pd.read_excel(arquivo)

    logger.info('Extraindo entidades relevantes das notícias...')
    ners = __get_NERs()
    writer = pd.ExcelWriter(os.path.join('ner.xlsx'), engine='xlsxwriter')

    for ner in ners:
        algoritmo = ner.get_nome_algoritmo()
        logger.info('Aplicando implementação ' + algoritmo)
        start_time = time.time()
        df_resultado = ner.extrair_entidades(df)
        logger.info("--- %s segundos ---" % (time.time() - start_time))
        df_resultado.to_excel(writer, sheet_name=algoritmo)

    writer.save()
    logger.info('Processamento concluído.')


def identificar_possiveis_empresas_citadas(caminho_arquivo='ner.xlsx'):
    """
    Executa o passo responsável por, a partir das entidades do tipo ORGANIZAÇÃO, identificar possíveis valores para os
    CNPJs dessas empresas, utilizando inicialmente busca textual.
    """
    df = pd.read_excel(caminho_arquivo)
    dao_rfb = DAO_RFB()
    resultado_analise = dict()
    data = link = midia = texto = titulo = ufs = None

    for i in range(len(df)):
        classificacao, data, entidade, link, midia, texto, titulo, ufs = __get_valores(df, i, data, link, midia, texto,
                                                                                      titulo, ufs)

        if (not pd.isna(entidade)) and classificacao == 'ORGANIZAÇÃO':
            # Remove acentuação, caracteres especiais e transforma para maiúsculas, para facilitar a busca.
            entidade_original = entidade
            entidade = processar_descricao_contratado(entidade)

            if len(entidade.strip()) > 0:
                # 1. Executa busca exata na base da Receita Federal
                empresas = __buscar_rfb(dao_rfb, data, entidade, entidade_original, link, midia, resultado_analise,
                                        texto, titulo, ufs)

                if len(empresas) == 0:
                    # 2. Busca no índice Lucene
                    __buscar_indice(data, entidade, entidade_original, link, midia, resultado_analise, texto, titulo,
                                    ufs)

    df = pd.concat(
        {k: pd.DataFrame(v, columns=['POSSÍVEIS EMPRESAS CITADAS', 'POSSÍVEIS CNPJs CITADOS', 'TIPO BUSCA']) for k, v in
         resultado_analise.items()})

    df.to_excel('com_empresas.xlsx')
    dao_rfb.encerrar_conexao()


def __get_valores(df, i, data, link, midia, texto, titulo, uf):
    if not pd.isna(df.iloc[i, 0]):
        titulo = df.iloc[i, 0]
    if not pd.isna(df.iloc[i, 1]):
        link = df.iloc[i, 1]
    if not pd.isna(df.iloc[i, 2]):
        midia = df.iloc[i, 2]
    if not pd.isna(df.iloc[i, 3]):
        data = df.iloc[i, 3]
    if not pd.isna(df.iloc[i, 4]):
        texto = df.iloc[i, 4]
    if not pd.isna(df.iloc[i, 5]):
        uf = df.iloc[i, 5]

    entidade = df.iloc[i, 7]
    classificacao = df.iloc[i, 8]

    return classificacao, data, entidade, link, midia, texto, titulo, uf


def __buscar_indice(data, entidade, entidade_original, link, midia, resultado_analise, texto, titulo, uf):
    map_empresa_to_cnpjs = defaultdict(list)
    resultados_no_indice = buscar_por_razao_social(entidade)
    if len(resultados_no_indice) > 0:
        tipo_busca = resultados_no_indice[0][2]

        for razao_social, cnpj, _ in resultados_no_indice:
            map_empresa_to_cnpjs[razao_social].append(cnpj)

        resultado_analise[(titulo, link, midia, data, texto, uf, entidade_original)] = [
            (razao_social, cnpjs, tipo_busca) for razao_social, cnpjs in
            map_empresa_to_cnpjs.items()]


def __buscar_rfb(dao_rfb, data, entidade, entidade_original, link, midia, resultado_analise, texto, titulo, uf):
    empresas = dao_rfb.buscar_empresa_por_razao_social(entidade)
    if len(empresas) > 0:
        map_empresa_to_cnpjs = defaultdict(list)
        for empresa in empresas:
            map_empresa_to_cnpjs[empresa[2]].append(empresa[0])

        resultado_analise[(titulo, link, midia, data, texto, uf, entidade_original)] = [
            (razao_social, cnpjs, 'BUSCA EXATA RFB') for razao_social, cnpjs in
            map_empresa_to_cnpjs.items()]
    return empresas


def obter_textos():
    """
    Obtém os textos de notícias da Internet.
    """
    logger = logging.getLogger('covidata')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    start_time = time.time()
    logger.info('Buscando as notícias na Internet...')
    arquivo_noticias = executar_busca()
    recuperar_textos(arquivo_noticias)
    logger.info("--- %s minutos ---" % ((time.time() - start_time) / 60))


if __name__ == '__main__':
    #extrair_entidades(os.path.join(config.diretorio_noticias, 'com_textos_baseline.xlsx'))
    identificar_possiveis_empresas_citadas()
