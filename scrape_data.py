############################ IMPORTS ############################

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import pandas as pd
import warnings
from io import StringIO

############################ FUNCTIONS ############################
def scrape_all_data(driver, ignored_exceptions, max_attempts=3):
    # List of pandas dfs to be returned
    dataframes = []
    
    # Loop condition variable
    has_next_table = True
    
    # DEBUG: Tables scraped counter
    table_num = 1
    
    # Load HTML to scrape data
    html = StringIO(driver.page_source)
    
    while(has_next_table):        
        ### Extract the HTML table ###
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
                print("Error whilst scraping table", table_num)
                print("No tables found in the HTML content.")
                break
                
        except IndexError:
            print("No table found on the page.")
            break
            
        except TimeoutException:
            print("Error whilst scraping table", table_num)
            print("Table element not found within the timeout period.")
            break
            
        except Exception as e:
            print("Error whilst scraping table", table_num)
            print(f"An error has occurred whilst loading table's outer HTML: {e}")
            break
        
        ### Load next button ###
        try:
            next_button = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(EC.presence_of_element_located((By.ID, "datatable_next")))
        except Exception as e:
            print("Error whilst scraping table", table_num)
            print(f"An error has occurred whilst loading next button with id datatable_next: {e}")
            break
            
        ### Click next button to go to next table ###
        try:
            next_button = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(EC.element_to_be_clickable((By.ID, "datatable_next")))
            driver.execute_script("arguments[0].click();", next_button)
        except Exception as e:
            print("Error whilst scraping table", table_num)
            print(f"An error has occurred whilst trying to click next button with id datatable_next: {e}")
            break
         
        ### Locate last button ###
        try:
            last_btn = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(EC.presence_of_element_located((By.ID, "datatable_last")))
            last_btn_classes = last_btn.get_attribute("class")
            
            # Update loop condition (whether there's more data to scrape or not)
            has_next_table = "disabled" not in last_btn_classes
        except Exception as e:
            print("Error whilst scraping table", table_num)
            print(f"An error has occurred whilst trying to locate last button with id datatable_last: {e}")
            break
            
        # DEBUG: Update table counter
        table_num += 1
        
        ### Wait for old element vars to go stale ###
        if has_next_table:
            try:
                WebDriverWait(driver, 10).until(EC.staleness_of(next_button))
            except Exception as e:
                print("Error whilst scraping table", table_num)
                print(f"An error has occurred whilst waiting for next_button to go stale: {e}")
                break
                
            try:
                WebDriverWait(driver, 10).until(EC.staleness_of(last_btn))
            except Exception as e:
                print("Error whilst scraping table", table_num)
                print(f"An error has occurred whilst waiting for last_button to go stale: {e}")
                break
        
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
    
    ############################ SCRAPE DATA ############################
    data = scrape_all_data(driver, ignored_exceptions)
    
    # Exit driver at the end of the loop
    driver.quit()
    
    num_tables = len(data)
    print("Total tables scraped:", num_tables)
    
    data = pd.concat(data)
    print(data)
    
    # TODO: compare number of rows from pandas to the number of rows that the text for id="datatable_info" shows.
    
    ############################ TRANSFORM/CLEAN DATA ############################
    # TODO: Clean data using pandas
    
    ############################ LOAD/SAVE DATA ############################
    # TODO: Load data into database (database TBD)
    
    # temporarily saving to CSV: 
    data.to_csv('out.csv')

if __name__ == "__main__":
    main()