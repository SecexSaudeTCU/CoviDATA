import pandas as pd

from covidata.cnpj import cnpj_util


def identificar_possiveis_empresas_citadas(caminho_arquivo='ner.xlsx'):
    """
    Executa o passo responsável por, a partir das entidades do tipo ORGANIZAÇÃO, identificar possíveis valores para os
    CNPJs dessas empresas, utilizando inicialmente busca textual.
    """
    df = pd.read_excel(caminho_arquivo)
    resultado_analise = dict()
    data = link = midia = texto = titulo = ufs = None

    for i in range(len(df)):
        classificacao, data, entidade, link, midia, texto, titulo, ufs = __get_valores(df, i, data, link, midia, texto,
                                                                                       titulo, ufs)

        if (not pd.isna(entidade)) and classificacao == 'ORGANIZAÇÃO':
            if len(entidade.strip()) > 0:
                map_empresa_to_cnpjs, tipo_busca = cnpj_util.buscar_empresas_por_razao_social(entidade)
                if len(map_empresa_to_cnpjs) > 0:
                    resultado_analise[(titulo, link, midia, data, texto, ufs, entidade)] = [
                        (razao_social, cnpjs, tipo_busca) for razao_social, cnpjs in map_empresa_to_cnpjs.items()]

    df = pd.concat(
        {k: pd.DataFrame(v, columns=['POSSÍVEIS EMPRESAS CITADAS', 'POSSÍVEIS CNPJs CITADOS', 'TIPO BUSCA']) for k, v in
         resultado_analise.items()})

    df.to_excel('com_empresas.xlsx')
    print('Processamento concluído.')


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

if __name__ == '__main__':
    identificar_possiveis_empresas_citadas()