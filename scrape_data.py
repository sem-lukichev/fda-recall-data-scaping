############################ IMPORTS ############################

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import warnings
from io import StringIO

############################ FUNCTIONS ############################
def scrape_data(driver, ignored_exceptions, max_attempts=3, save_to_csv=False):
    # List of pandas dfs to be returned
    dataframes = []
    # Loop condition variable
    has_next_table = True
    # DEBUG: Tables scraped counter
    table_num = 1
    
    while(has_next_table):
        print("Table:",table_num)
        
        # Extract the HTML table
        try:
            # Load HTML to scrape data
            html = StringIO(driver.page_source)

            # Read HTML with pandas
            dfs = pd.read_html(html)

            # Add df to list of dfs
            if dfs: # Table found
                df = dfs[0]
                dataframes.append(df)
            else: # Table not found
                print("No tables found in the HTML content.")
                
        except IndexError:
            print("No table found on the page.")
            
        except TimeoutException:
            print("Table element not found within the timeout period.")
            
        except Exception as e:
            print("Uh oh.")
            print(f"An error has occurred whilst loading table's outer HTML: {e}")
        
        # Load next button
        try:
            next_button = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(EC.presence_of_element_located((By.ID, "datatable_next")))
        except Exception as e:
            print("Uh oh.")
            print(f"An error has occurred whilst loading next button with id datatable_next: {e}")
            
        # Click next button to go to next table
        try:
            next_button = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(EC.element_to_be_clickable((By.ID, "datatable_next")))
            driver.execute_script("arguments[0].click();", next_button)
        except Exception as e:
            print("Uh oh.")
            print(f"An error has occurred whilst trying to click next button with id datatable_next: {e}")
         
        # Locate last button
        try:
            last_btn = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(EC.presence_of_element_located((By.ID, "datatable_last")))
            last_btn_classes = last_btn.get_attribute("class")
            
            # Update loop condition (whether there's more data to scrape or not)
            has_next_table = "disabled" not in last_btn_classes
        except Exception as e:
            print("Uh oh.")
            print(f"An error has occurred whilst trying to locate last button with id datatable_last: {e}")
            
        # DEBUG: Update table counter
        table_num += 1
        
        # Wait for old element vars to go stale
        try:
            if has_next_table:
                WebDriverWait(driver, 10).until(EC.staleness_of(next_button))
        except Exception as e:
            print("Uh oh.")
            print(f"An error has occurred whilst waiting for next_button to go stale: {e}")
            
        try:
            if has_next_table:
                WebDriverWait(driver, 10).until(EC.staleness_of(last_btn))
        except Exception as e:
            print("Uh oh.")
            print(f"An error has occurred whilst waiting for last_button to go stale: {e}")
            
        
    return(dataframes) 

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
    driver = webdriver.Chrome(options=options)
    driver.get(URL)
    
    # TODO: Scrape data
    data = scrape_data(driver, ignored_exceptions)
    print("Total tables scraped:",len(data))
    data = pd.concat(data)
    print(data)
    
    # Exit driver at the end of the loop
    driver.quit()   
    
    # TODO: Transform data
    
    # TODO: Load data into database

if __name__ == "__main__":
    main()