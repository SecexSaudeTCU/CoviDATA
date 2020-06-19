"""
Script para scraping dos dados do portal de transparência do TCE-RJ.

O Portal do TCE-RJ utiliza o Power BI. Após
"""
import requests
import json

# URL utilizada pelo Power BI para obnteção de dados mostrados no painel do TCE-RJ
url = 'https://wabi-brazil-south-api.analysis.windows.net/public/reports/querydata?synchronous=true'

# Cabeçalhos da requisção POST
headers = {"Host": "wabi-brazil-south-api.analysis.windows.net", "Connection": "keep-alive", "Content-Length": "5686", "Accept": "application/json, text/plain, */*", "RequestId": "9df85402-7893-05d0-7d11-db4e62e17f73", "X-PowerBI-ResourceKey": "9926f3ef-e5a3-4ae7-a300-3c0027b04e32", "Content-Type": "application/json;charset=UTF-8", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36", "ActivityId": "81bc4a2a-3f9c-485c-a7a2-9154c1a1e600", "Origin": "https://app.powerbi.com", "Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty", "Referer": "https://app.powerbi.com/view?r=eyJrIjoiOTkyNmYzZWYtZTVhMy00YWU3LWEzMDAtM2MwMDI3YjA0ZTMyIiwidCI6IjJjYmJlYmU0LTc2MzgtNDYxYi05ZjhjLTE2MmVkZGMzZDBlNCJ9", "Accept-Encoding": "gzip, deflate, br", "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,de;q=0.6,it;q=0.5,fr;q=0.4,es;q=0.3"}

# A requisição do PowerBI para obtenção de dados inclui um campo "CacheKey", localizado aproximadamente na posição 2559
# que quando incluido no payload da requisição POST estava gerando erro de decodificação
payload = """{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"d","Entity":"DimCompraDireta","Type":0},{"Name":"d1","Entity":"DimTempo","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Processo"},"Name":"DimCompraDireta.Processo"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Objeto"},"Name":"DimCompraDireta.Objeto"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Afastamento"},"Name":"DimCompraDireta.Afastamento"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Data Aprovação"},"Name":"DimCompraDireta.DataAprovacao"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"EnquadramentoLegal"},"Name":"DimCompraDireta.EnquadramentoLegal"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Unidade"},"Name":"DimCompraDireta.Unidade"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"FornecedorVencedor"},"Name":"DimCompraDireta.FornecedorVencedor"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Qtd"},"Name":"DimCompraDireta.Qtd"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"ValorProcesso"},"Name":"Sum(DimCompraDireta.ValorProcesso)"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"UnidadeMedida"},"Name":"DimCompraDireta.UnidadeMedida"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Item"},"Name":"DimCompraDireta.Item"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"ValorUnitario"},"Name":"DimCompraDireta.ValorUnitario"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"MesProcesso"}}],"Values":[[{"Literal":{"Value":"'4'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"AnoProcesso"}}],"Values":[[{"Literal":{"Value":"'2020'"}}]]}}},{"Condition":{"Not":{"Expression":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d1"}},"Property":"ANO"}}],"Values":[[{"Literal":{"Value":"null"}}],[{"Literal":{"Value":"'2019'"}}],[{"Literal":{"Value":"'2020'"}}],[{"Literal":{"Value":"'2021'"}}]]}}}}}],"OrderBy":[{"Direction":2,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Data Aprovação"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5,6,7,8,9,10,11],"Subtotal":1}]},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"Version":1},"ExecutionMetricsKind":3}}]},"CacheKey":"{\"Commands\":[{\"SemanticQueryDataShapeCommand\":{\"Query\":{\"Version\":2,\"From\":[{\"Name\":\"d\",\"Entity\":\"DimCompraDireta\",\"Type\":0},{\"Name\":\"d1\",\"Entity\":\"DimTempo\",\"Type\":0}],\"Select\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Processo\"},\"Name\":\"DimCompraDireta.Processo\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Objeto\"},\"Name\":\"DimCompraDireta.Objeto\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Afastamento\"},\"Name\":\"DimCompraDireta.Afastamento\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Data Aprovação\"},\"Name\":\"DimCompraDireta.DataAprovacao\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"EnquadramentoLegal\"},\"Name\":\"DimCompraDireta.EnquadramentoLegal\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Unidade\"},\"Name\":\"DimCompraDireta.Unidade\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"FornecedorVencedor\"},\"Name\":\"DimCompraDireta.FornecedorVencedor\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Qtd\"},\"Name\":\"DimCompraDireta.Qtd\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"ValorProcesso\"},\"Name\":\"Sum(DimCompraDireta.ValorProcesso)\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"UnidadeMedida\"},\"Name\":\"DimCompraDireta.UnidadeMedida\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Item\"},\"Name\":\"DimCompraDireta.Item\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"ValorUnitario\"},\"Name\":\"DimCompraDireta.ValorUnitario\"}],\"Where\":[{\"Condition\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"MesProcesso\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"'4'\"}}]]}}},{\"Condition\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"AnoProcesso\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"'2020'\"}}]]}}},{\"Condition\":{\"Not\":{\"Expression\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d1\"}},\"Property\":\"ANO\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"null\"}}],[{\"Literal\":{\"Value\":\"'2019'\"}}],[{\"Literal\":{\"Value\":\"'2020'\"}}],[{\"Literal\":{\"Value\":\"'2021'\"}}]]}}}}}],\"OrderBy\":[{\"Direction\":2,\"Expression\":{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"Data Aprovação\"}}}]},\"Binding\":{\"Primary\":{\"Groupings\":[{\"Projections\":[0,1,2,3,4,5,6,7,8,9,10,11],\"Subtotal\":1}]},\"DataReduction\":{\"DataVolume\":3,\"Primary\":{\"Window\":{\"Count\":500}}},\"Version\":1},\"ExecutionMetricsKind\":3}}]}","QueryId":"","ApplicationContext":{"DatasetId":"aa0cf697-6d6f-4a7d-9c2b-b9f22a7f5d10","Sources":[{"ReportId":"c5975969-be74-4d79-9b83-5ffb81a758e2"}]}}],"cancelQueries":[],"modelId":2583990}"""
payload_no_cache = """{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"d","Entity":"DimCompraDireta","Type":0},{"Name":"d1","Entity":"DimTempo","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Processo"},"Name":"DimCompraDireta.Processo"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Objeto"},"Name":"DimCompraDireta.Objeto"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Afastamento"},"Name":"DimCompraDireta.Afastamento"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Data Aprovação"},"Name":"DimCompraDireta.DataAprovacao"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"EnquadramentoLegal"},"Name":"DimCompraDireta.EnquadramentoLegal"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Unidade"},"Name":"DimCompraDireta.Unidade"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"FornecedorVencedor"},"Name":"DimCompraDireta.FornecedorVencedor"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Qtd"},"Name":"DimCompraDireta.Qtd"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"ValorProcesso"},"Name":"Sum(DimCompraDireta.ValorProcesso)"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"UnidadeMedida"},"Name":"DimCompraDireta.UnidadeMedida"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Item"},"Name":"DimCompraDireta.Item"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"ValorUnitario"},"Name":"DimCompraDireta.ValorUnitario"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"MesProcesso"}}],"Values":[[{"Literal":{"Value":"'4'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"AnoProcesso"}}],"Values":[[{"Literal":{"Value":"'2020'"}}]]}}},{"Condition":{"Not":{"Expression":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"d1"}},"Property":"ANO"}}],"Values":[[{"Literal":{"Value":"null"}}],[{"Literal":{"Value":"'2019'"}}],[{"Literal":{"Value":"'2020'"}}],[{"Literal":{"Value":"'2021'"}}]]}}}}}],"OrderBy":[{"Direction":2,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Data Aprovação"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5,6,7,8,9,10,11],"Subtotal":1}]},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"Version":1},"ExecutionMetricsKind":3}}]},"QueryId":"","ApplicationContext":{"DatasetId":"aa0cf697-6d6f-4a7d-9c2b-b9f22a7f5d10","Sources":[{"ReportId":"c5975969-be74-4d79-9b83-5ffb81a758e2"}]}}],"cancelQueries":[],"modelId":2583990}"""

# Realizar a requisição POST para obtençaõ
r = requests.post(url, headers=headers, data=payload_no_cache.encode('utf-8'))

# Salva conteúdo retornado em arquivo de teste
with open('teste.txt', 'w') as f:
    f.write(str(json.loads(r.content)))
