import locale

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager


def get_browser(url):
    """
    Retorna uma instância de driver Selenium, aberta em uma respectiva página.
    :param url: URL para a página a ser aberta.
    :return: A instância criada e configurada para representar o navegador (Selenium driver).
    """
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument('--headless')

    locale.setlocale(locale.LC_ALL, "pt_br")

    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
    driver.get(url)
    return driver
