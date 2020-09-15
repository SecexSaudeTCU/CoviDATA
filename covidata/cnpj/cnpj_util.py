import json

import requests

from covidata import config


def buscar_empresas_por_razao_social(razao_social):
    resultado = json.loads(requests.get(config.url_api_cnpj + razao_social).content)
    map_empresa_to_cnpjs = resultado['dados']['empresas']
    tipo_busca = resultado['dados']['tipo_busca']
    return map_empresa_to_cnpjs, tipo_busca
