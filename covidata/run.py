"""
Extrai os dados de todos os portais para os quais o respectivo scrapper já foi implementado e salva o resultado
na pasta dados, na raiz do projeto. Os novos scrapers devem ser adicionados ao script manualmente.
"""

import os
import sys

# Adiciona diretorio raiz ao PATH. Devido a ausência de setup.py, isto garante que as importações sempre funcionarão
diretorio_raiz = os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
sys.path.append(diretorio_raiz)

from covidata.webscraping.scrappers.AM import uf_am
from covidata.webscraping.scrappers.CE import uf_ce
from covidata.webscraping.scrappers.PA import uf_pa, pt_belem
from covidata.webscraping.scrappers.RJ import pt_rj_capital, tce_rj
from covidata.webscraping.scrappers.PE import pt_pe_capital
from covidata.webscraping.scrappers.AP import uf_ap
from covidata.webscraping.scrappers.RR import uf_rr
from covidata.webscraping.scrappers.RO import uf_ro
from covidata.webscraping.scrappers.AC import uf_ac
from covidata.webscraping.scrappers.ES import uf_es
from covidata.webscraping.scrappers.SP import tcm_sp, pt_sp, pt_sp_capital
from covidata.webscraping.scrappers.AL import uf_al
from covidata.webscraping.scrappers.TO import uf_to
from covidata.webscraping.scrappers.BA import uf_ba
from covidata.webscraping.scrappers.MT import uf_mt
from covidata.webscraping.scrappers.GO import uf_go
from covidata.webscraping.scrappers.RS import uf_rs, pt_rs_capital
from covidata.webscraping.scrappers.PR import uf_pr
from covidata.webscraping.scrappers.SC import uf_sc
from covidata.webscraping.scrappers.MG import uf_mg
from covidata.webscraping.scrappers.MS import uf_ms
import logging
import time

if __name__ == '__main__':
    logger = logging.getLogger('covidata')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    start_time = time.time()

    logger.info('# Recuperando dados do Acre...')
    uf_ac.main()

    logger.info('# Recuperando dados de Alagoas...')
    uf_al.main()

    logger.info('# Recuperando dados do Amapá...')
    uf_ap.main()

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

    logger.info('# Recuperando dados de Mato Grosso...')
    uf_mt.main()

    logger.info('# Recuperando dados de Mato Grosso do Sul...')
    uf_ms.main()

    logger.info('# Recuperando dados de Minas Gerais...')
    uf_mg.main()

    logger.info('# Recuperando dados do Pará...')
    uf_pa.main()
    pt_belem.main()

    logger.info('# Recuperando dados do Paraná...')
    uf_pr.main()

    logger.info('# Recuperando dados de Pernambuco...')
    pt_pe_capital.main()

    logger.info('# Recuperando dados do Rio de Janeiro...')
    tce_rj.main()
    pt_rj_capital.main()

    logger.info('# Recuerando dados do Rio Grande do Sul...')
    uf_rs.main()
    #TODO: Removido temporariamente devido a um bug no salvamento de arquivos Excel no Windows.
    #pt_rs_capital.main()

    logger.info('# Recuperando dados de Rondônia...')
    uf_ro.main()

    logger.info('# Recuperando dados de Roraima...')
    uf_rr.main()

    logger.info('# Recuperando dados de Santa Catarina...')
    uf_sc.main()

    logger.info('# Recuperando dados de São Paulo...')
    pt_sp.main()
    pt_sp_capital.main()
    tcm_sp.main()

    logger.info('# Recuperando dados de Tocantins...')
    uf_to.main()

    logger.info("--- %s minutos ---" % ((time.time() - start_time) / 60))
