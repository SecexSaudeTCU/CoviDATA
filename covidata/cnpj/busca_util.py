import re

import unidecode


def processar_descricao_contratado(descricao):
    descricao = descricao.strip()
    descricao = descricao.upper()

    # Remove espaços extras
    descricao = re.sub(' +', ' ', descricao)

    # Remove acentos
    descricao = unidecode.unidecode(descricao)

    # Remova caracteres especiais
    descricao = descricao.replace('&', '').replace('/', '').replace('-', '').replace('"', '')

    return descricao
