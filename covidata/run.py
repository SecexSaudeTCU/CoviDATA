"""
Extrai os dados de todos os portais para os quais o respectivo scrapper já foi implementado e salva o resultado
na pasta dados, na raiz do projeto. Os novos scrapers devem ser adicionados ao script manualmente.
"""
import datetime
import logging
import os
import sys
import time
import traceback
from collections import defaultdict

import pandas as pd

from covidata import config
from covidata.persistencia.consolidacao import salvar
from covidata.webscraping.scrappers.AC.PT_AC import PT_AC_Scraper
from covidata.webscraping.scrappers.AC.PT_RioBranco import PT_RioBranco_Scraper
from covidata.webscraping.scrappers.AC.TCE_AC import TCE_AC_ContratosScraper, TCE_AC_DespesasScraper, \
    TCE_AC_ContratosMunicipiosScraper, TCE_AC_DespesasMunicipiosScraper
from covidata.webscraping.scrappers.AL import uf_al
from covidata.webscraping.scrappers.AM import uf_am
from covidata.webscraping.scrappers.AP import uf_ap
from covidata.webscraping.scrappers.AP.PT_AP import PT_AP_Scraper
from covidata.webscraping.scrappers.BA import uf_ba
from covidata.webscraping.scrappers.CE import uf_ce
from covidata.webscraping.scrappers.DF.PT_DF import PT_DF_Scraper
from covidata.webscraping.scrappers.ES import uf_es
from covidata.webscraping.scrappers.GO import uf_go
from covidata.webscraping.scrappers.MA import uf_ma
from covidata.webscraping.scrappers.MA.TCE_MA import TCE_MA_Scraper
from covidata.webscraping.scrappers.MG import uf_mg
from covidata.webscraping.scrappers.MS import uf_ms
from covidata.webscraping.scrappers.MS.PT_MS import PT_MS_Scraper
from covidata.webscraping.scrappers.MT.PT_MT import PT_MT_Scraper
from covidata.webscraping.scrappers.PA import uf_pa
from covidata.webscraping.scrappers.PA.PT_Belem import PT_Belem_Scraper
from covidata.webscraping.scrappers.PB import uf_pb
from covidata.webscraping.scrappers.PE import pt_pe_capital
from covidata.webscraping.scrappers.PI import uf_pi
from covidata.webscraping.scrappers.PR import uf_pr
from covidata.webscraping.scrappers.PR.PT_PR_aquisicoes import PT_PR_AquisicoesScraper
from covidata.webscraping.scrappers.RJ import uf_rj
from covidata.webscraping.scrappers.RN import uf_rn
from covidata.webscraping.scrappers.RN.PT_RN import PT_RN_Scraper
from covidata.webscraping.scrappers.RO import uf_ro
from covidata.webscraping.scrappers.RR import uf_rr
from covidata.webscraping.scrappers.RR.PT_RR import PT_RR_Scraper
from covidata.webscraping.scrappers.RS import uf_rs
from covidata.webscraping.scrappers.SC import uf_sc
from covidata.webscraping.scrappers.SE import uf_se
from covidata.webscraping.scrappers.SE.PT_SE import PT_SE_Scraper
from covidata.webscraping.scrappers.SP import uf_sp
# Adiciona diretorio raiz ao PATH. Devido a ausência de setup.py, isto garante que as importações sempre funcionarão
from covidata.webscraping.scrappers.TO.PT_TO import PT_TO_Scraper

diretorio_raiz = os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
sys.path.append(diretorio_raiz)

if __name__ == '__main__':
    logger = logging.getLogger('covidata')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    # No momento, estão nesta lista os scrapers mais instáveis, que apresentam erros intermitentes.
    map_ufs_scrapers = {
        'AC': [PT_AC_Scraper(config.url_pt_AC), PT_RioBranco_Scraper(config.url_pt_RioBranco),
               TCE_AC_ContratosScraper(config.url_tce_AC_contratos),
               TCE_AC_DespesasScraper(config.url_tce_AC_despesas),
               TCE_AC_ContratosMunicipiosScraper(config.url_tce_AC_contratos_municipios),
               TCE_AC_DespesasMunicipiosScraper(config.url_tce_AC_despesas_municipios)],
        'AP': [PT_AP_Scraper(config.url_pt_AP)],
        'DF': [PT_DF_Scraper(config.url_pt_DF)],
        'MA': [TCE_MA_Scraper(config.url_tce_MA)],
        'MS': [PT_MS_Scraper(config.url_pt_MS)],
        'MT': [PT_MT_Scraper(config.url_pt_MT)],
        'PA': [PT_Belem_Scraper(config.url_pt_Belem)],
        'PR': [PT_PR_AquisicoesScraper()],
        'RN': [PT_RN_Scraper(config.url_pt_RN)],
        'RR': [PT_RR_Scraper(config.url_pt_RR)],
        'SE': [PT_SE_Scraper(config.url_pt_SE)],
        'TO': [PT_TO_Scraper(config.url_pt_TO)],
    }
    df_consolidado = defaultdict(pd.DataFrame)
    erros = []

    for uf, scrapers in map_ufs_scrapers.items():
        for scraper in scrapers:
            try:
                scraper.scrap()
                data_extracao = datetime.datetime.now()
                df, salvo = scraper.consolidar(data_extracao)
                df_consolidado[uf] = df_consolidado[uf].append(df)

                if not salvo:
                    salvar(df_consolidado[uf], uf)
            except:
                traceback.print_exc()
                erros.append(scraper.url)

    start_time = time.time()

    logger.info('# Recuperando dados de Alagoas...')
    uf_al.main()

    logger.info('# Recuperando dados do Amapá...')
    uf_ap.main(df_consolidado['AP'])

    logger.info('# Recuperando dados do Amazonas...')
    uf_am.main()

    logger.info('# Recuperando dados da Bahia...')
    uf_ba.main()

    logger.info('# Recuperando dados do Ceará...')
    uf_ce.main()

    logger.info('# Recuperando dados do Espírito Santo...')
    uf_es.main()

    logger.info('# Recuperando dados de Goiás...')
    uf_go.main()

    logger.info('# Recuperando dados do Maranhão...')
    uf_ma.main(df_consolidado['MA'])

    logger.info('# Recuperando dados de Mato Grosso do Sul...')
    uf_ms.main(df_consolidado['MS'])

    logger.info('# Recuperando dados de Minas Gerais...')
    uf_mg.main()

    logger.info('# Recuperando dados do Pará...')
    uf_pa.main(df_consolidado['PA'])

    logger.info('# Recuperando dados de Paraíba...')
    uf_pb.main()

    logger.info('# Recuperando dados de Piauí...')
    uf_pi.main()

    logger.info('# Recuperando dados do Paraná...')
    uf_pr.main(df_consolidado['PR'])

    logger.info('# Recuperando dados de Pernambuco...')
    pt_pe_capital.main()

    logger.info('# Recuperando dados do Rio de Janeiro...')
    uf_rj.main()

    logger.info('# Recuperando dados de Rio Grande do Norte...')
    uf_rn.main(df_consolidado['RN'])

    logger.info('# Recuperando dados do Rio Grande do Sul...')
    uf_rs.main()
    # TODO: Acesso disponível apenas por meio de API
    # pt_rs_capital.main() (acesso disponível apenas por meio de API)

    logger.info('# Recuperando dados de Rondônia...')
    uf_ro.main()

    logger.info('# Recuperando dados de Roraima...')
    uf_rr.main(df_consolidado['RR'])

    logger.info('# Recuperando dados de Santa Catarina...')
    uf_sc.main()

    logger.info('# Recuperando dados de Sergipe...')
    uf_se.main(df_consolidado['SE'])

    logger.info('# Recuperando dados de São Paulo...')
    uf_sp.main()

    logger.info("--- %s minutos ---" % ((time.time() - start_time) / 60))

    logger.info('Erros ocorridos:')
    logger.info(erros)
