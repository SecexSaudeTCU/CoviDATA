from covidata.webscraping.scrappers.AM import uf_am
from covidata.webscraping.scrappers.CE import uf_ce
from covidata.webscraping.scrappers.PA import uf_pa, pt_belem
from covidata.webscraping.scrappers.RJ import pt_rj_capital, tce_rj
from covidata.webscraping.scrappers.PE import pt_recife
from covidata.webscraping.scrappers.AP import uf_ap
from covidata.webscraping.scrappers.RR import uf_rr
from covidata.webscraping.scrappers.RO import uf_ro
from covidata.webscraping.scrappers.AC import uf_ac
from covidata.webscraping.scrappers.ES import uf_es
from covidata.webscraping.scrappers.SP import tcm_sp, pt_sp, pt_sp_capital
from covidata.webscraping.scrappers.AL import uf_al
from covidata.webscraping.scrappers.RJ import tce_rj
from covidata.webscraping.scrappers.TO import uf_to
import logging

if __name__ == '__main__':
    logger = logging.getLogger('covidata')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    logger.info('# Recuperando dados do Acre...')
    uf_ac.main()

    logger.info('# Recuperando dados do Amapá...')
    uf_ap.main()

    logger.info('# Recuperando dados do Amazonas...')
    uf_am.main()

    logger.info('# Recuperando dados do Ceará...')
    uf_ce.main()

    logger.info('# Recuperando dados do Espírito Santo...')
    uf_es.main()

    logger.info('# Recuperando dados do Pará...')
    uf_pa.main()
    pt_belem.main()

    """
    logger.info('# Recuperando dados de Pernambuco...')
    pt_recife.main()
    """

    logger.info('# Recuperando dados do Rio de Janeiro...')
    tce_rj.main()
    pt_rj_capital.main()

    logger.info('# Recuperando dados de Rondônia...')
    uf_ro.main()

    logger.info('# Recuperando dados de Roraima...')
    uf_rr.main()

    logger.info('# Recuperando dados de São Paulo...')
    pt_sp.main()
    pt_sp_capital.main()
    # tcm_sp.main()

    logger.info('# Recuperando dados de Alagoas...')
    uf_al.main()

    logger.info('# Recuperando dados de Tocantins...')
    uf_to.main()
