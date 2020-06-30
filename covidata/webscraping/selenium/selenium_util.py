import locale

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager


def get_browser(url):
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument('--headless')

    locale.setlocale(locale.LC_ALL, "pt_br")

    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
    driver.get(url)
    return driver