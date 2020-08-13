import datetime
import pandas as pd
from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper
from os import path


class PT_PR_AquisicoesScraper(Scraper):
    def __init__(self):
        super().__init__('')
        agora = datetime.datetime.now()
        mes_atual = agora.month

        if mes_atual <= 9:
            mes_atual = '0' + str(mes_atual)

        ano_atual = agora.year
        self.nome_arquivo_aquisicoes = 'Contratos%20Aquisi%C3%A7%C3%B5es_0.xls'
        self.url = f'{config.url_pt_PR}{ano_atual}-{mes_atual}/{self.nome_arquivo_aquisicoes}'

    def scrap(self):
        self.download_aquisicoes_contratacoes()

    def consolidar(self, data_extracao):
        return self.__consolidar_aquisicoes(data_extracao)

    def download_aquisicoes_contratacoes(self):

        pt_PR_aquisicoes = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia'),
                                          self.url,
                                          self.nome_arquivo_aquisicoes)
        pt_PR_aquisicoes.download()

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
        planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia',
                                      'Contratos%20Aquisi%C3%A7%C3%B5es_0.xls')
        df_original = pd.read_excel(planilha_original, header=6, sheet_name='UNIFICAÇÃO DOS 6 MESES')
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + self.url
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'PR', '', data_extracao)

        # Como no caso de PR, em nenhum momento a informação de CNPJ é fornecida, sinalizar tipo para PJ para que, na
        # consolidação geral seja executada a busca por CNPJs.
        df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        return df
