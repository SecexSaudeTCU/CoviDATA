import requests
import json


def get_municipios_por_uf(sigla_uf):
    """
    Retorna um dicionário que mapeia nomes de municípios nos respectivos códigos.
    :param sigla_uf: A sigla da unidade da federação.
    :return: Dicionário que mapeia nomes de municípios (em caracteres maiúsculos) nos respectivos códigos.
    """
    id_uf = __get_id_uf_por_sigla(sigla_uf)

    retorno = requests.get(f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{id_uf}/municipios')
    resultado = json.loads(retorno.content)

    mapa_municipio_id = dict()

    for municipio in resultado:
        mapa_municipio_id[municipio['nome'].upper()] = municipio['id']

    return mapa_municipio_id


def __get_id_uf_por_sigla(sigla):
    # Recupera a lista de todas as UFs
    retorno = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/estados')
    resultado = json.loads(retorno.content)

    for estado in resultado:
        if estado['sigla'] == sigla:
            return estado['id']

    return None
