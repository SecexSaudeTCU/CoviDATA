from os import path

import pandas as pd
import zipfile

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_PR_Scraper(Scraper):
    def scrap(self):
        csv_zip = 'DISPENSAS_INEXIGIBILIDADE_COVID-2020_CSV.zip'
        diretorio = config.diretorio_dados.joinpath('PR').joinpath('portal_transparencia')

        downloader = FileDownloader(diretorio, config.url_pt_PR, csv_zip)
        downloader.download()

        with zipfile.ZipFile(csv_zip, 'r') as zip_ref:
            zip_ref.extractall(diretorio)

    def consolidar(self, data_extracao):
        return self.__consolidar_aquisicoes(data_extracao), False

    def __consolidar_aquisicoes(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'orgao', consolidacao.UG_DESCRICAO: 'orgao',
                            consolidacao.DESPESA_DESCRICAO: 'objeto',
                            consolidacao.ITEM_EMPENHO_QUANTIDADE: 'quantidade',
                            consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'valor_unitario',
                            consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'valor_total_item',
                            consolidacao.VALOR_CONTRATO: 'valor_total_solicitacao',
                            consolidacao.SITUACAO: 'situacao_solicitacao', consolidacao.NUMERO_CONTRATO: 'num_contrato',
                            consolidacao.ANO: 'ano_contrato', consolidacao.DATA_PUBLICACAO: 'data_publicacao',
                            consolidacao.ITEM_EMPENHO_DESCRICAO: 'descricao_item',
                            consolidacao.CONTRATADO_CNPJ: 'cpf_cnpj_fornecedor',
                            consolidacao.CONTRATADO_DESCRICAO: 'fornecedor'
                            }
        colunas_adicionais = ['sigla', 'tempo_prazo_contrato', 'unidade_tempo_prazo_contrato', 'ordem_de_compra',
                              'ano_ordem_de_compra', 'situacao_ordem_compra', 'situacao_item_ordem_compra',
                              'ordem_de_servico', 'ano_ordem_de_servico', 'situacao_ordem_servico',
                              'situacao_item_ordem_compra', 'situacao_contrato', 'numero_solicitacao',
                              'ano_solicitacao', 'modalidade', 'protocolo', 'natureza', 'descricao_publicacao',
                              'nr_publicacao', 'numero_item', 'data_avaliacao', 'cotacoes']
        planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia',
                                      'TB_DISPENSAS_INEXIGIBILIDADE-2020.csv')
        df_original = pd.read_csv(planilha_original)

        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               config.url_pt_PR, 'PR', '', data_extracao)

        df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        return df
