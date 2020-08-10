from covidata.noticias.contratados.indice_contratados import buscar
from covidata.municipios.ibge import get_municipios, get_estados

# Lista de todos os municípios do país
municipios = get_municipios()

# Lista de todos os estados do país
estados = get_estados()


def filtrar_contratados(entidades_texto, busca_exata=True, num_min_caracteres=3):
    resultado = []

    for termo, classificacao in entidades_texto:
        termo = termo.strip()
        # Se o termo não se refere a uma localidade brasileira
        if termo.upper() not in estados and termo.upper() not in municipios and termo.upper() != 'BRASIL' and len(
                termo) > num_min_caracteres:
            if busca_exata:
                termo = '"' + termo + '"'
            resultados_busca = buscar(termo)

            if len(resultados_busca) > 0:
                resultado.append((termo, classificacao, set(resultados_busca)))

    return resultado
