"""
Arquivo de definição de constantes e variáveis globais utilizadas em todo o projeto.
"""

import pathlib

# Define o caminho do diretório onde os dados serão armazenados
diretorio_raiz = pathlib.Path(__file__).parent.parent
diretorio_dados = diretorio_raiz.joinpath('dados')

# Urls de portais de tansparência do governos estaduais
url_pt_AM = 'http://www.transparencia.am.gov.br/covid-19/contratos/'
url_pt_CE = 'https://cearatransparente.ce.gov.br/files/downloads/transparency/coronavirus/gasto_covid_dados_abertos.xlsx'
url_pt_PA = 'https://transparenciacovid19.pa.gov.br/covid.json'
url_pt_AP = 'http://cosiga.ap.gov.br/api/contratos_covidV1?termo=&orgao='
url_pt_RR = 'http://transparencia.rr.gov.br/covid/public/relatorios/despesas/covid19/xls?_token=MbNzpM8JNT4g6D6wYE0QW5cL0Q0qS2KaNn6f5HJa&exercicio=2020&uo=20601&dsuo=Fundo%20Estadual%20de%20Sa%C3%BAde&key=%242y%2410%249foy18cHU84Zv7F1KIW4zuYEH4oPEpFovVkUW7NXUkfNVOWMQHe.i'
url_pt_RO = 'http://comprasemergenciais-covid19.ro.gov.br/Grafico/DespesasDiretasToCSV'
url_pt_AC = 'http://covid19.ac.gov.br/financeiro/empenhos/csv'
url_pt_ES = 'https://coronavirus.es.gov.br/Media/Coronavirus/Transparencia/DadosAbertos/dados-contratos-emergenciais-covid-19.csv?v=49'
url_pt_SP = 'https://www.saopaulo.sp.gov.br/coronavirus/transparencia/'
url_pt_AL = 'http://transparencia.al.gov.br:8080/transparencia_relatorios/transparencia_relatorios/despesa/json-despesa-covid19-itens?offset=0&tipo=xls&nome_contratado=&nota_empenho=&processo=&contrato=&objeto=&orgao=&codigo_elemento=&'
url_pt_MA = 'http://www.transparencia.ma.gov.br/app/compras/covid#lista'
url_pt_TO = 'http://www.gestao.cge.to.gov.br/projetos/contratos_covid/consulta_contrato_covid_2/'
url_pt_BA = 'http://www.saude.ba.gov.br/temasdesaude/coronavirus/contratacoes-covid19/'
url_pt_MT = 'http://consultas.transparencia.mt.gov.br/compras/contratos_covid/'
url_pt_GO = 'http://www.monitoramentoegestao.com.br/download_arquivos/aquisicoes.csv'
url_pt_RS = 'https://www.compras.rs.gov.br/transparencia/editais-covid19.csv?contexto=Celic'
url_pt_PR = 'http://www.coronavirus.pr.gov.br/sites/cadastrocovid19/arquivos_restritos/files/documento/'
url_pt_SC_contratos = 'http://dados.sc.gov.br/dataset/dfc9ffcd-0f0d-4d92-9b5f-30f2d718105d/resource/18a4438d-4352-45f0-bd97-9edfd4425ae2/download/contrato_item.xlsx'
url_pt_SC_despesas = 'http://www.transparencia.sc.gov.br/data/despesa/exportcsv?anomesinifiltro%5B%5D=202001&anomesfimfiltro%5B%5D=202012&agrupamentos%5B%5D=orgao&indicador=3779&'
url_pt_MG = 'http://www.transparencia.mg.gov.br/covid-19/compras-contratos'
url_pt_MS = 'http://www.comprascoronavirus.ms.gov.br/'

# URLs de portais de transparência do governo estadual do Acre
url_tce_AC_contratos = 'http://www.tce.ac.gov.br/covid19/Contratos-Estado.html'
url_tce_AC_despesas = 'http://www.tce.ac.gov.br/covid19/despesa_estado_html.html'
url_tce_AC_contratos_municipios = 'http://www.tce.ac.gov.br/covid19/Contratos-Municipios.html'
url_tce_AC_despesas_municipios = 'http://www.tce.ac.gov.br/covid19/despesas_municipios_html.html'
url_tce_MA = 'https://www6.tce.ma.gov.br/sacop/muralsite/muralcovid.zul'
url_tce_RS = 'http://www1.tce.rs.gov.br/aplicprod/f?p=50500:16::XLS::::&cs=3Hn8jXEOKZFSR4U62TfOfBk7Yeas'

# URLSs de portais de transparência de govenos municipais (capitais)
url_pt_Manaus = 'https://covid19.manaus.am.gov.br/transparencia-covid-19/'
url_pt_Fortaleza = 'https://transparencia.fortaleza.ce.gov.br/index.php/despesa/despesasCovid19'
url_pt_Belem = 'http://contratoemergencial.belem.pa.gov.br/despesas/'
url_pt_Rio_favorecidos = 'http://riotransparente.rio.rj.gov.br/arquivos/Open_Data_Favorecidos_Covid19_2020.xlsx'
url_pt_Rio_contratos = 'http://riotransparente.rio.rj.gov.br/arquivos/Open_Data_Contratos_Covid19_2020.xlsx'
url_pt_Rio_despesas_por_ato = 'http://riotransparente.rio.rj.gov.br/arquivos/Open_Data_Desp_Ato_Covid19_2020.txt'
url_pt_Recife = 'http://transparencia.recife.pe.gov.br/codigos/web/estaticos/estaticos.php?nat=COV#filho'
url_pt_Macapa = 'http://transparencia2.macapa.ap.gov.br/tipo_contrato/covid-19/'
url_pt_BoaVista = 'https://transparencia.boavista.rr.gov.br/cpl/export/csv/covid-19'
url_pt_PortoVelho_despesas_por_instituicao = 'https://ecidadetransparencia.portovelho.ro.gov.br/files/tmp/1592514776.csv'
url_pt_PortoVelho_despesas_por_credor = 'https://ecidadetransparencia.portovelho.ro.gov.br/files/tmp/1592514860.csv'
url_pt_PortoVelho_tipos_despesas = 'https://ecidadetransparencia.portovelho.ro.gov.br/files/tmp/1592514918.csv'
url_pt_RioBranco = 'http://transparencia.riobranco.ac.gov.br/calamidade-publica/ver-abreviacao/covid-19/'
url_pt_Vitoria = 'http://transparencia.vitoria.es.gov.br/Licitacao.Lista.RelatorioExcel.ashx?municipioID=1&numero=0&exercicio=2020&idsModalidade=0&idComissao=0&idEnquadramento=0&idsSituacao=0&idSecretaria=0&idResponsavel=0&idRequisitante=0&idsFornecedor=0&tipo=tlmDispensa&objeto=&emergencial=covid-19&pesquisa='
url_pt_SaoPaulo = 'https://www.prefeitura.sp.gov.br/cidade/secretarias/controladoria_geral/transparencia_covid19/index.php?p=295874'
url_pt_Maceio = 'http://www.licitacao.maceio.al.gov.br/covid/json'  # fonte: http://www.licitacao.maceio.al.gov.br/
url_pt_SaoLuis = 'http://covid19.saoluis.ma.gov.br/2802'
url_pt_Cuiaba = 'http://covid.cuiaba.mt.gov.br/publico/busca/13'
url_pt_PortoAlegre = 'http://www.portaldecompraspublicas.com.br/18/Processos/?slT=3&slO=206&slU=233325&slTag=COVID19'
url_pt_Curitiba_contratacoes = 'https://mid.curitiba.pr.gov.br/conteudos/coronavirus/transparencia/licitacoes_contratacoes.csv'
url_pt_Curitiba_aquisicoes = 'https://www.transparencia.curitiba.pr.gov.br/sgp/DespesasCovid19.aspx'
url_pt_Florianopolis = 'http://portal.pmf.sc.gov.br/transparencia/arquivos/Aquisicao%20e%20contratos.csv'
url_pt_BeloHorizonte = 'https://prefeitura.pbh.gov.br/sites/default/files/estrutura-de-governo/controladoria/transparencia/covid19/contratacaocorona.xlsx'
url_pt_CampoGrande = 'https://transparenciacovid.campogrande.ms.gov.br/despesas/'

# URLs dos portais de transparência dos Tribunais de Contas Municipais
url_tcm_PA_1 = 'http://nie-tcmpa.droppages.com/files/Argus%20TCMPA%20-%20Fornecedores%20por%20Valor%20Homologado.xlsx'
url_tcm_PA_2 = 'http://nie-tcmpa.droppages.com/files/Argus%20TCMPA%20-%20Fornecedores%20por%20Quantidade%20de%20Munic%C3%ADpios.xlsx'
url_tcm_SP = 'https://mobile.tcm.sp.gov.br/Licitacao/ListarCovid?showMenu=0'
url_tcm_GO = 'https://www.tcmgo.tc.br/coronavirus/'
