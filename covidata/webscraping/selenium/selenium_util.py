import locale

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

def configurar_browser():
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument('--headless')
    locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
    return driver
