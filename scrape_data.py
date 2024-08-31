############################ IMPORTS ############################

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC
import warnings

############################ FUNCTIONS ############################

def init_web_driver(options, url):
        # Initialize driver
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        return driver

############################ MAIN FUNCTION ############################

def main():
    
    URL = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"

    # Optimized webdriver settings
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--log-level=3")
    
    # Warning supression
    warnings.simplefilter(action='ignore', category=FutureWarning)
    ignored_exceptions=(NoSuchElementException,StaleElementReferenceException,)
    
    # Initialize web driver
    driver = init_web_driver(options, URL)
    
    # TODO: Scrape data
    
    # TODO: Transform data
    
    # TODO: Load data into database

if __name__ == "__main__":
    main()