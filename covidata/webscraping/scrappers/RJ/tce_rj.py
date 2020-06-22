"""
Script para scraping da tabela principal da seção "Compras Diretas SIGA" do Portal de Dados TCE-RJ, disponível em:
    https://app.powerbi.com/view?r=eyJrIjoiOTkyNmYzZWYtZTVhMy00YWU3LWEzMDAtM2MwMDI3YjA0ZTMyIiwidCI6IjJjYmJlYmU0LTc2MzgtNDYxYi05ZjhjLTE2MmVkZGMzZDBlNCJ9

O Portal do TCE-RJ utiliza o Power BI. Os dados do painel são armazenados nos servidores da Microsoft e obtidos por
meio de chamadas assíncronas e retornadas no formato JSON. O formato JSON está compactado. A compactação é feita por
meio de dois mecanismos:

    * Os registros que possuem valores iguais a valores de registros anteriores (em qualquer coluna, na ordem do próprio
     JSON) são armazenados de forma reduzida. Somente os valores diferentes do registro anterior são enviados. As
     colunas com valores repetidos são indicadas em uma máscara binária disponível na chave "R" do registro. A chave
     binária é armazenada em número inteiro. Para obtenção da máscara à partir do número inteiro, deve-se:
        1. Converter o número interio para binário
        2. Adicionar zeros à esquerda, de forma a haver 12 dígitos binários
        3. Inverter a ordem  dos dígitos binários (i.e. o dígito binário mais à direita corresponde à primeira coluna).
    Os dígitos iguais a 1 correspondem às colunas cujos valores devem ser repetidos do registro anterior. Os dígitos
    iguais a zero corresondem às colunas cujos valores constam do registro. Os detalhes para descompactação estão
    comentados linha à linha no código.

    * Os valores textuais são substituídos por valores numéricos cujo significados estão descritos no dicionário
    constante do caminho no JSON correspondente a: ['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']

"""
import requests
import json
import pandas as pd
from datetime import datetime, timedelta


# URL utilizada pelo Power BI para obnteção de dados mostrados no painel do TCE-RJ
url = 'https://wabi-brazil-south-api.analysis.windows.net/public/reports/querydata?synchronous=true'

# Cabeçalhos da requisção POST
headers = {"Host": "wabi-brazil-south-api.analysis.windows.net", "Connection": "keep-alive", "Content-Length": "5686", "Accept": "application/json, text/plain, */*", "RequestId": "9df85402-7893-05d0-7d11-db4e62e17f73", "X-PowerBI-ResourceKey": "9926f3ef-e5a3-4ae7-a300-3c0027b04e32", "Content-Type": "application/json;charset=UTF-8", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36", "ActivityId": "81bc4a2a-3f9c-485c-a7a2-9154c1a1e600", "Origin": "https://app.powerbi.com", "Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty", "Referer": "https://app.powerbi.com/view?r=eyJrIjoiOTkyNmYzZWYtZTVhMy00YWU3LWEzMDAtM2MwMDI3YjA0ZTMyIiwidCI6IjJjYmJlYmU0LTc2MzgtNDYxYi05ZjhjLTE2MmVkZGMzZDBlNCJ9", "Accept-Encoding": "gzip, deflate, br", "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,de;q=0.6,it;q=0.5,fr;q=0.4,es;q=0.3"}

# A requisição do PowerBI para obtenção de dados inclui um campo "CacheKey", localizado aproximadamente na posição 2559
# que quando incluído no payload da requisição POST estava gerando erro de decodificação. A seção foi excluída e não
# houve prejuízo para obtenção dos dados.
payload = """{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"d","Entity":"DimCompraDireta","Type":0},{"Name":"d1","Entity":"DimTempo","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Processo"},"Name":"DimCompraDireta.Processo"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Objeto"},"Name":"DimCompraDireta.Objeto"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Afastamento"},"Name":"DimCompraDireta.Afastamento"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Data Aprovação"},"Name":"DimCompraDireta.DataAprovacao"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"EnquadramentoLegal"},"Name":"DimCompraDireta.EnquadramentoLegal"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Unidade"},"Name":"DimCompraDireta.Unidade"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"FornecedorVencedor"},"Name":"DimCompraDireta.FornecedorVencedor"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Qtd"},"Name":"DimCompraDireta.Qtd"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"ValorProcesso"},"Name":"Sum(DimCompraDireta.ValorProcesso)"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"UnidadeMedida"},"Name":"DimCompraDireta.UnidadeMedida"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Item"},"Name":"DimCompraDireta.Item"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"ValorUnitario"},"Name":"DimCompraDireta.ValorUnitario"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"MesProcesso"}}],"Values":[[{"Literal":{"Value":"'4'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"AnoProcesso"}}],"Values":[[{"Literal":{"Value":"'2020'"}}]]}}},{"Condition":{"Not":{"Expression":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d1"}},"Property":"ANO"}}],"Values":[[{"Literal":{"Value":"null"}}],[{"Literal":{"Value":"'2019'"}}],[{"Literal":{"Value":"'2020'"}}],[{"Literal":{"Value":"'2021'"}}]]}}}}}],"OrderBy":[{"Direction":2,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Data Aprovação"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5,6,7,8,9,10,11],"Subtotal":1}]},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"Version":1},"ExecutionMetricsKind":3}}]},"CacheKey":"{\"Commands\":[{\"SemanticQueryDataShapeCommand\":{\"Query\":{\"Version\":2,\"From\":[{\"Name\":\"d\",\"Entity\":\"DimCompraDireta\",\"Type\":0},{\"Name\":\"d1\",\"Entity\":\"DimTempo\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Processo\"},\"Name\":\"DimCompraDireta.Processo\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Objeto\"},\"Name\":\"DimCompraDireta.Objeto\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Afastamento\"},\"Name\":\"DimCompraDireta.Afastamento\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Data Aprovação\"},\"Name\":\"DimCompraDireta.DataAprovacao\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"EnquadramentoLegal\"},\"Name\":\"DimCompraDireta.EnquadramentoLegal\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Unidade\"},\"Name\":\"DimCompraDireta.Unidade\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"FornecedorVencedor\"},\"Name\":\"DimCompraDireta.FornecedorVencedor\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Qtd\"},\"Name\":\"DimCompraDireta.Qtd\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"ValorProcesso\"},\"Name\":\"Sum(DimCompraDireta.ValorProcesso)\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"UnidadeMedida\"},\"Name\":\"DimCompraDireta.UnidadeMedida\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Item\"},\"Name\":\"DimCompraDireta.Item\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"ValorUnitario\"},\"Name\":\"DimCompraDireta.ValorUnitario\"}],\"Where\":[{\"Condition\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"MesProcesso\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"'4'\"}}]]}}},{\"Condition\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"AnoProcesso\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"'2020'\"}}]]}}},{\"Condition\":{\"Not\":{\"Expression\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d1\"}},\"Property\":\"ANO\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"null\"}}],[{\"Literal\":{\"Value\":\"'2019'\"}}],[{\"Literal\":{\"Value\":\"'2020'\"}}],[{\"Literal\":{\"Value\":\"'2021'\"}}]]}}}}}],\"OrderBy\":[{\"Direction\":2,\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Data Aprovação\"}}}]},\"Binding\":{\"Primary\":{\"Groupings\":[{\"Projections\":[0,1,2,3,4,5,6,7,8,9,10,11],\"Subtotal\":1}]},\"DataReduction\":{\"DataVolume\":3,\"Primary\":{\"Window\":{\"Count\":500}}},\"Version\":1},\"ExecutionMetricsKind\":3}}]}","QueryId":"","ApplicationContext":{"DatasetId":"aa0cf697-6d6f-4a7d-9c2b-b9f22a7f5d10","Sources":[{"ReportId":"c5975969-be74-4d79-9b83-5ffb81a758e2"}]}}],"cancelQueries":[],"modelId":2583990}"""
payload_sem_cache = """{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"d","Entity":"DimCompraDireta","Type":0},{"Name":"d1","Entity":"DimTempo","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Processo"},"Name":"DimCompraDireta.Processo"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Objeto"},"Name":"DimCompraDireta.Objeto"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Afastamento"},"Name":"DimCompraDireta.Afastamento"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Data Aprovação"},"Name":"DimCompraDireta.DataAprovacao"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"EnquadramentoLegal"},"Name":"DimCompraDireta.EnquadramentoLegal"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Unidade"},"Name":"DimCompraDireta.Unidade"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"FornecedorVencedor"},"Name":"DimCompraDireta.FornecedorVencedor"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Qtd"},"Name":"DimCompraDireta.Qtd"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"ValorProcesso"},"Name":"Sum(DimCompraDireta.ValorProcesso)"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"UnidadeMedida"},"Name":"DimCompraDireta.UnidadeMedida"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Item"},"Name":"DimCompraDireta.Item"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"ValorUnitario"},"Name":"DimCompraDireta.ValorUnitario"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"MesProcesso"}}],"Values":[[{"Literal":{"Value":"'4'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"AnoProcesso"}}],"Values":[[{"Literal":{"Value":"'2020'"}}]]}}},{"Condition":{"Not":{"Expression":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d1"}},"Property":"ANO"}}],"Values":[[{"Literal":{"Value":"null"}}],[{"Literal":{"Value":"'2019'"}}],[{"Literal":{"Value":"'2020'"}}],[{"Literal":{"Value":"'2021'"}}]]}}}}}],"OrderBy":[{"Direction":2,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Data Aprovação"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5,6,7,8,9,10,11],"Subtotal":1}]},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"Version":1},"ExecutionMetricsKind":3}}]},"QueryId":"","ApplicationContext":{"DatasetId":"aa0cf697-6d6f-4a7d-9c2b-b9f22a7f5d10","Sources":[{"ReportId":"c5975969-be74-4d79-9b83-5ffb81a758e2"}]}}],"cancelQueries":[],"modelId":2583990}"""

# Realizar a requisição POST para obtenção dos dados da tabela principal da seção  "Compras Diretas SIGA"
r = requests.post(url, headers=headers, data=payload_sem_cache.encode('utf-8'))

# Obtém os dados em formato JSON com o conteúdo da tabela
d = json.loads(r.content)

# Extrai do JSON o campo com as linhas da tabela
dados = d['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']

# Extrai o dicionário que mapeia valores numéricos para os respectivos textos (usado para compactação)
value_dicts = d['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']


def completa_registro(registro_anterior, registro_atual, valor_r):
    """
    Retorna o registro completo extraído do registro atual e do registro anteior utilizando a máscara binária (valor_r)
    que indica quais valores devem ser obtidos do registro anterior e quais contam do registro atual.
    """

    # Número de colunas no registro
    NUM_COLUNAS = 12
    assert(len(registro_anterior) == NUM_COLUNAS)  # O registro anterior precisa estar completo (i.e. ter os 12 valores)

    # Converte o valor R na máscara binária que indica a repetição dos valores (os 2 primeiros caractees são '0b')
    mascara_binaria = list(bin(valor_r))[2::]

    # Preenche a máscara com zeros à esquerda para seu tamanho ser sempre igual a 12
    mascara_binaria = (NUM_COLUNAS - len(mascara_binaria)) * ['0'] + mascara_binaria

    # Reverte a ordem da máscara (a ordem deve ser lida da direita para a esquerda (após a adiçaõ de 0's a esquerda))
    mascara_binaria.reverse()

    # Inicializa lista para armaenar os valores do registro
    valores_registro_atual = []

    # Contador que indica quantos valores foram obtidos do registro anterior
    counter_valores_repetidos = 0

    # Para i variando de 0 a NUM_COLUNAS
    for i in range(0, NUM_COLUNAS):

        # Caso o valor seja igual a 1, indicando que a coluna deve ser repetida do registro anterior
        if mascara_binaria[i] == '1':

            # Adiciona ao registro o mesmo valor do registro anterior
            valores_registro_atual.append(registro_anterior[i])

            # Incrementa contador de registros que foram extraídos do registro anterior
            counter_valores_repetidos += 1

        else:

            # Adiciona ao registro atual o valor constante do registro atual
            valores_registro_atual.append(registro_atual[i - counter_valores_repetidos])

    # Retorna registro com os valores completos obtidos do registro atual e do posterior
    return valores_registro_atual


# Inicializa a lista de registros com o primeiro elemento (que é completo) adicionado à lista
registros_completos = [dados[0]['C']]

# Para cada registro, a partir do segundo
for registro_atual in dados[1::]:

    # Tenta extrair a chave 'R' do registro atual
    try:
        # Extrai o "R" do regitro atual (que indica quais valores são repetidos do registro anterior)
        valor_r = registro_atual['R']

        # Obtém registro anterior (último registro adicionado). Este registro já está "completo" (i.e. já processado)
        registro_anterior_completo = registros_completos[-1]

        # Obtém o registro atual cujos dados precisam ser completados com dados obtidos do registro anterior
        registro_atual_incompleto = registro_atual['C']

        # Obtém registro completo, a partir das regras de duplicação de valores extraídos do registro precedente
        registro_atual_completo = completa_registro(registro_anterior_completo, registro_atual_incompleto, valor_r)

    # Caso não haja a chave R no registro atual (isto ocorre quando todos os campos são distintos do anterior)
    except KeyError:

        # Extrai o registro completo (neste caso, não há valores repetidos obtidos do registro anterior)
        registro_atual_completo = registro_atual['C']

    # Adicionar registro completo à lista
    registros_completos.append(registro_atual_completo)


# Cria dataframe com os dados obtidos
colunas = ["Processo", "Objeto", "Afastamento", "Data Aprovação", "Enquadramento Legal", "Unidade", "Fornecedor", "Qtd", "Valor Processo", "Unidade Medida", "Item", "Valor Unitário"]
df = pd.DataFrame(data=registros_completos, columns=colunas)

# Dict indicando o nome do "mapa" (outro dict) de valores numéricos para descrições textuais
mapa_coluna_dicionario = {
    'Processo': 'D0',
    'Objeto': 'D1',
    'Afastamento': 'D2',
    'Enquadramento Legal': 'D3',
    'Unidade': 'D4',
    'Fornecedor': 'D5',
    'Qtd': 'D6',
    'Unidade Medida': 'D7',
    'Item': 'D8'
}

# As colunas 'Data Aprovação', 'Valor Processo' e 'Valor Unitário' não possuem dicionário associado
colunas_mapeadas = ["Processo", "Objeto", "Afastamento", "Enquadramento Legal", "Unidade", "Fornecedor", "Qtd", "Unidade Medida", "Item"]

# Para cada coluna do dataframe
for coluna in colunas_mapeadas:

    # Obtém dicionário correspondente à coluna
    dicionario = value_dicts[mapa_coluna_dicionario[coluna]]

    # Converte valores numéricos para textuais
    df[coluna] = [valor if type(valor) == str else dicionario[valor] for valor in df[coluna]]


# Função para adequar o formato de timestamp constante do JSON para formato mostrado no painel
# Três zeros são removidos da direita (div por 1000), é adicoinado 1 dia e o formato é ajustado
def ajusta_data(t):
    return (datetime.fromtimestamp(int(t)/1000) + timedelta(days=1)).strftime('%Y-%m-%d')


# Converte os timestamps para datas no formato correto
df['Data Aprovação'] = [ajusta_data(t) for t in df['Data Aprovação']]

# Salva dados em planilha do excel
agora = datetime.now().strftime('%Y-%m-%d %Hh%Mm%Ss')
df.to_excel(f'tce_rj_compras_diretas_{agora}.xlsx')


