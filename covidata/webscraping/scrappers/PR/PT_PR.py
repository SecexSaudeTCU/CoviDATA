import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper
from os import path


class PT_PR_Aquisicoes_Scraper(Scraper):
    def scrap(self):
        page = requests.get(self.url)
        soup = BeautifulSoup(page.content, 'html.parser')
        article = soup.findAll('article',{'about':'/Campanha/Pagina/Transparencia-Aquisicoes-e-Contratacoes-Contratos'})
        li = article[0].find_all('ul')[0].find_all('li')[1]
        link = li.find_all('a')[0].attrs['href']
        pt_PR_aquisicoes = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia'),
                                          link, 'contratos.xls')
        pt_PR_aquisicoes.download()

    def consolidar(self, data_extracao):
        return self.__consolidar_aquisicoes(data_extracao), False

    def __consolidar_aquisicoes(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ORGÃO - CONTRATANTE/MUNICÍPIO',
                            consolidacao.UG_DESCRICAO: 'ORGÃO - CONTRATANTE/MUNICÍPIO',
                            consolidacao.CONTRATADO_DESCRICAO: 'EMPRESA CONTRATADA/CNPJ ',
                            consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                            consolidacao.ITEM_EMPENHO_QUANTIDADE: 'QUANTIDADE POR UNIDADES / DIÁRIAS',
                            consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'VALOR    UNITÁRIO (R$)',
                            consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'VALOR                          TOTAL (R$)',
                            consolidacao.DATA_PUBLICACAO: 'DATA  PUBLICAÇÃO                       DIOE / FOLHA'}
        colunas_adicionais = ['PRAZO              CONTRATO (mês)', 'NÚMERO DISPENSA', 'PROTOCOLO / PROCESSO']
        planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'contratos.xls')
        #df_original = pd.read_excel(planilha_original, header=6, sheet_name='UNIFICAÇÃO DOS 6 MESES')

        #Obtém a última aba da planilha
        xl = pd.ExcelFile(planilha_original)
        aba = xl.sheet_names[-1]
        df_original = pd.read_excel(planilha_original, header=6, sheet_name=aba)

        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + self.url
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'PR', '', data_extracao)

        # Como no caso de PR, em nenhum momento a informação de CNPJ é fornecida, sinalizar tipo para PJ para que, na
        # consolidação geral seja executada a busca por CNPJs.
        df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        return df


class PT_PR_DadosAbertos_Scraper(Scraper):
    def __init__(self):
        super().__init__('')
        agora = datetime.datetime.now()
        mes_atual = agora.month
        ano_atual = agora.year

        if mes_atual <= 9:
            mes_atual = '0' + str(mes_atual)

        self.nome_arquivo_dados_abertos = 'dados_abertos.xlsx'
        self.url = f'{config.url_pt_PR_dados_abertos}{ano_atual}-{mes_atual}/{self.nome_arquivo_dados_abertos}'

    def scrap(self):
        pt_PR_dados_abertos = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia'),
                                             self.url, self.nome_arquivo_dados_abertos)
        pt_PR_dados_abertos.download()

    def consolidar(self, data_extracao):
        return self.__consolidar_dados_abertos(data_extracao), False

    def __consolidar_dados_abertos(self, data_extracao):
        dicionario_dados = {consolidacao.ANO: 'ANO', consolidacao.DOCUMENTO_NUMERO: 'EMPENHO',
                            consolidacao.FUNCAO_COD: 'FUNÇÃO', consolidacao.SUBFUNCAO_COD: 'SUBFUNÇÃO',
                            consolidacao.PROGRAMA_COD: 'PROGRAMA', consolidacao.MOD_APLICACAO_COD: 'MODALIDADE',
                            consolidacao.ELEMENTO_DESPESA_COD: 'ELEMENTO',
                            consolidacao.SUB_ELEMENTO_DESPESA_COD: 'SUBELEMENTO',
                            consolidacao.FONTE_RECURSOS_COD: 'FONTE',
                            consolidacao.VALOR_EMPENHADO: 'EMPENHADO', consolidacao.VALOR_LIQUIDADO: 'LIQUIDADO',
                            consolidacao.VALOR_PAGO: 'PAGO', consolidacao.CATEGORIA_ECONOMICA_COD: 'CATEGORIA',
                            consolidacao.CONTA_CORRENTE: 'CONTA CORRENTE', consolidacao.ESPECIE: 'ESPECIE'}
        colunas_adicionais = ['MÊS', 'PODER', 'UNIDADE CONTÁBIL', 'UNIDADE ORÇAMENTÁRIA', 'P_A_OE', 'NATUREZA',
                              'NATUREZA2',
                              'OBRA_META', 'HISTORICO', 'Modalidade2']
        planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia',
                                      self.nome_arquivo_dados_abertos)
        df_original = pd.read_excel(planilha_original)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + self.url
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'PR', '', data_extracao, self.pos_processar_dados_abertos)
        return df

    def pos_processar_dados_abertos(self, df):
        df[consolidacao.ANO] += 2000
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

        # Como no caso de PR, em nenhum momento a informação de CNPJ é fornecida, sinalizar tipo para PJ para que, na
        # consolidação geral seja executada a busca por CNPJs.
        df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        return df
