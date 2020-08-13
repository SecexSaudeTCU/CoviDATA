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
from covidata.webscraping.scrappers.AC import uf_ac
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
from covidata.webscraping.scrappers.MT import uf_mt
from covidata.webscraping.scrappers.PA import uf_pa
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
    scrapers = {'AP': PT_AP_Scraper(config.url_pt_AP), 'PR': PT_PR_AquisicoesScraper(),
                'MS': PT_MS_Scraper(config.url_pt_MS), 'RR': PT_RR_Scraper(config.url_pt_RR),
                'RN': PT_RN_Scraper(config.url_pt_RN), 'MA': TCE_MA_Scraper(config.url_tce_MA),
                'DF': PT_DF_Scraper(config.url_pt_DF), 'SE': PT_SE_Scraper(config.url_pt_SE),
                'TO': PT_TO_Scraper(config.url_pt_TO)}
    dfs_consolidados = defaultdict(list)
    erros = []

    for uf, scraper in scrapers.items():
        try:
            scraper.scrap()
            data_extracao = datetime.datetime.now()
            df = scraper.consolidar(data_extracao)
            dfs_consolidados[uf].append(df)
        except:
            traceback.print_exc()
            erros.append(scraper.url)

    start_time = time.time()
    logger.info('# Recuperando dados do Acre...')
    uf_ac.main()

    logger.info('# Recuperando dados de Alagoas...')
    uf_al.main()

    logger.info('# Recuperando dados do Amapá...')
    if len(dfs_consolidados['AP']) > 0:
        uf_ap.main(dfs_consolidados['AP'][0])
    else:
        uf_ap.main(pd.DataFrame())

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
    if len(dfs_consolidados['MA']) > 0 and dfs_consolidados['MA'][0]:
        uf_ma.main(dfs_consolidados['MA'][0])
    else:
        uf_ma.main(pd.DataFrame())

    logger.info('# Recuperando dados de Mato Grosso...')
    uf_mt.main()

    logger.info('# Recuperando dados de Mato Grosso do Sul...')
    if len(dfs_consolidados['MS']) > 0:
        uf_ms.main(dfs_consolidados['MS'][0])
    else:
        uf_ms.main(pd.DataFrame())

    logger.info('# Recuperando dados de Minas Gerais...')
    uf_mg.main()

    logger.info('# Recuperando dados do Pará...')
    uf_pa.main()

    logger.info('# Recuperando dados de Paraíba...')
    uf_pb.main()

    logger.info('# Recuperando dados de Piauí...')
    uf_pi.main()

    logger.info('# Recuperando dados do Paraná...')
    if len(dfs_consolidados['PR']) > 0:
        uf_pr.main(dfs_consolidados['PR'][0])
    else:
        uf_pr.main(pd.DataFrame())

    logger.info('# Recuperando dados de Pernambuco...')
    pt_pe_capital.main()

    logger.info('# Recuperando dados do Rio de Janeiro...')
    uf_rj.main()

    logger.info('# Recuperando dados de Rio Grande do Norte...')
    if len(dfs_consolidados['RN']) > 0:
        uf_rn.main(dfs_consolidados['RN'][0])
    else:
        uf_rn.main(pd.DataFrame())

    logger.info('# Recuperando dados do Rio Grande do Sul...')
    uf_rs.main()
    # TODO: Acesso disponível apenas por meio de API
    # pt_rs_capital.main() (acesso disponível apenas por meio de API)

    logger.info('# Recuperando dados de Rondônia...')
    uf_ro.main()

    logger.info('# Recuperando dados de Roraima...')
    if len(dfs_consolidados['RR']) > 0:
        uf_rr.main(dfs_consolidados['RR'][0])
    else:
        uf_rr.main(pd.DataFrame())

    logger.info('# Recuperando dados de Santa Catarina...')
    uf_sc.main()

    logger.info('# Recuperando dados de Sergipe...')
    if len(dfs_consolidados['SE']) > 0:
        uf_se.main(dfs_consolidados['SE'][0])
    else:
        uf_se.main(pd.DataFrame())

    logger.info('# Recuperando dados de São Paulo...')
    uf_sp.main()

    logger.info("--- %s minutos ---" % ((time.time() - start_time) / 60))

    logger.info('Erros ocorridos:')
    logger.info(erros)
