import json
import os
import requests

from covidata import config


def get_municipios_por_uf(sigla_uf):
    """
    Retorna um dicionário que mapeia nomes de municípios nos respectivos códigos.
    :param sigla_uf: A sigla da unidade da federação.
    :return: Dicionário que mapeia nomes de municípios (em caracteres maiúsculos) nos respectivos códigos.
    """
    id_uf = __get_id_uf_por_sigla(sigla_uf)

    arquivo_fallback = str(id_uf) + '.json'
    resultado = __get(config.url_api_ibge + '/' + str(id_uf) + '/municipios', arquivo_fallback)

    # Salva o arquivo como cache, em caso de indisponibilidade futura da API.
    dirname = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dirname, arquivo_fallback), 'w') as outfile:
        json.dump(resultado, outfile)

    mapa_municipio_id = dict()

    for municipio in resultado:
        mapa_municipio_id[municipio['nome'].upper()] = municipio['id']

    return mapa_municipio_id


def get_codigo_municipio_por_nome(nome, sigla_uf):
    """
    Retorna o código de um município a partir do seu nome e da sigla do estado ao qual pertence.

    :param nome: O nome do município.
    :param sigla_uf: A sigla da unidade da federação.
    """
    municipios = get_municipios_por_uf(sigla_uf)
    return municipios[nome.upper()]


def __get_id_uf_por_sigla(sigla):
    # Recupera a lista de todas as UFs
    resultado = __get(config.url_api_ibge, 'estados.json')

    for estado in resultado:
        if estado['sigla'] == sigla:
            return estado['id']

    return None


def __get(uri, arquivo_local):
    resultado = None
    try:
        retorno = requests.get(uri, timeout=10)
        resultado = json.loads(retorno.content)
    except requests.exceptions.ReadTimeout:
        # Utiliza cache local
        dirname = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dirname, arquivo_local)) as json_file:
            resultado = json.load(json_file)
    return resultado
