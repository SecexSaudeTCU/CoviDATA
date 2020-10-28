from os import path
import pandas as pd
import time

import logging

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_Manaus_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Manaus = PortalTransparencia_Manaus()
        pt_Manaus.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_materiais_capital(data_extracao), False

    def consolidar_materiais_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO',
                            consolidacao.DESPESA_DESCRICAO: 'MATERIAL/SERVIÇO',
                            consolidacao.VALOR_CONTRATO: 'VLR TOTAL CONTRATADO',
                            consolidacao.CONTRATADO_DESCRICAO: 'FORNECEDOR', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.DOCUMENTO_NUMERO: 'NOTA DE EMPENHO'}
        df_original = pd.read_csv(
            path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'Manaus',
                      'PÚBLICA-CONTROLE-PROCESSOS-COMBATE-COVID-19-MATERIAIS.csv'))
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Manaus, 'AM',
                               get_codigo_municipio_por_nome('Manaus', 'AM'), data_extracao,
                               self.pos_processar_materiais_capital)
        return df

    def pos_processar_materiais_capital(self,df):
        # Elimina as 17 últimas linhas, que só contêm totalizadores e legendas
        df.drop(df.tail(17).index, inplace=True)

        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Manaus'
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

        return df


# TODO: Em tese, esta carga não precisaria ser via Selenium, porém tem a vantagem de abstrair a URL direta do arquivo,
#  que no momento contém, por exemplo, "2020/06".
class PortalTransparencia_Manaus(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'Manaus'),
                         config.url_pt_Manaus)

    def _executar(self):
        button = self.driver.find_element_by_id('btn_csv')
        button.click()
