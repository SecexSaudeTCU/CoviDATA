"""
Rotina de extração de notícias da aba News do Google a partir de string de busca e dias de início e fim.
"""

import pandas as pd
import urllib
from datetime import date
from GoogleNews import GoogleNews


def extrai_noticias_google(q, dia_inicio, dia_fim, num_limite_paginas=1, lang='pt-BR'):
    """
    Retorna data frame com as notícias obtidas na aba News do Google

    Parâmetros
    ----------
        q : str
            String de busca

        data_inicio, dta_fim : datatime.Date
            Datas de início e fim para realização da busca

        num_limite_num_limite_paginas : int
            Número máxima de páginas que serão obtidas.

        lang : str
            Código da lingua para realização da busca (padrão pt-BR)

    Retorno
    -------
        resultados : DataFrame
            Dataframe com os reulstados de busca
    """

    # String de busca formatado adequadamente para URL
    q = urllib.parse.quote(q)

    # Strings com as datas no formato esperado pela lib GoogleNews
    formato_data = '%m/%d/%Y'
    dia_inicio_formatado = dia_inicio.strftime(formato_data)
    dia_fim_formatado = dia_fim.strftime(formato_data)

    # Instancia interface de busca ao Google News com idioma pt-BR e período adequado
    gn = GoogleNews(lang=lang, start=dia_inicio_formatado, end=dia_fim_formatado)

    # Inicializa lista para armazenar os resultados de busca
    resultados = []

    # Realiza busca da primeira página
    print(f'Buscando página 1')
    gn.search(q)
    print(len(gn.result()), gn.result())
    resultados = resultados + gn.result()
    gn.clear()

    # Para a página 2 em diante (p2 corresponde ao índice 1)
    for i in range(2, num_limite_paginas + 1):

        print(f'Buscando página {i}')

        # Busca a página
        gn.getpage(i)

        # Adiciona reusltado à lista
        resultados = resultados + gn.result()

        print(len(gn.result()), gn.result())

        # Enterrompe a execução se nenhum resultado tiver sido retornado na última página
        if gn.result() == []:
            print(f'A consulta à página {i} não retornou nehnum resulto')
            break

        # Apaga cache do resultado
        gn.clear()

    # Cria e retorna dataframe
    return pd.DataFrame(resultados)


if __name__ == '__main__':

    # String de busca (sugestão do Samuel)
    q = 'fraude and (aquisição or contratação)'

    # Datas de início e fim (1º de abril de 2020 até o dia corrente)
    dia_inicio = date.fromisoformat('2020-04-01')
    dia_fim = date.today()

    # Número limite de páginas
    num_limite_paginas = 100

    # Realiza busca
    df = extrai_noticias_google(q, dia_inicio, dia_fim, num_limite_paginas=num_limite_paginas)

    # Salva resultados
    df.to_excel(f'noticias_n_{len(df)}.xlsx')

