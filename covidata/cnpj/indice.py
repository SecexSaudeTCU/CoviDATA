import json

import requests


def buscar_por_razao_social(razao_social):
    retorno = []

    # Busca inicialmente pela string exata (entre aspas)
    prefixo_url = 'http://localhost:8090/search?query=razao_social:'
    url = prefixo_url + '"' + razao_social + '"'
    resultado = json.loads(requests.get(url).content)
    registros = resultado['records']

    for registro in registros:
        retorno.append((registro['razao_social'], registro['cnpj']))

    # Se a busca exata não retornou nenhum resultado, tenta a busca sem aspas
    if len(retorno) == 0:
        url = prefixo_url + razao_social
        resultado = json.loads(requests.get(url).content)
        registros = resultado['records']

        for registro in registros:
            retorno.append((registro['razao_social'], registro['cnpj']))

    return retorno
