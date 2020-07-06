import requests
import json
from covidata import config


def get_municipios_por_uf(sigla_uf):
    """
    Retorna um dicionário que mapeia nomes de municípios nos respectivos códigos.
    :param sigla_uf: A sigla da unidade da federação.
    :return: Dicionário que mapeia nomes de municípios (em caracteres maiúsculos) nos respectivos códigos.
    """
    id_uf = __get_id_uf_por_sigla(sigla_uf)

    retorno = requests.get(config.url_api_ibge + '/' + str(id_uf) + '/municipios')
    resultado = json.loads(retorno.content)

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

    retorno = requests.get(config.url_api_ibge)
    resultado = json.loads(retorno.content)

    for estado in resultado:
        if estado['sigla'] == sigla:
            return estado['id']

    return None
