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
import duckdb
from datetime import date

############################ FUNCTIONS ############################
# TODO: Scrape only new data function

def scrape_data(driver, ignored_exceptions, most_recent_date=None):
    # List of pandas dfs to be returned
    dataframes = []
    
    # Loop condition variables
    flag = True
    has_recent_date = most_recent_date is not None
    
    # DEBUG: Tables scraped counter
    table_num = 1
    
    # Load HTML to scrape data
    html = StringIO(driver.page_source)
    
    while(flag):        
        ### Extract the HTML table ###
        try:
            # Load HTML to scrape data
            html = StringIO(driver.page_source)

            # Read HTML with pandas
            dfs = pd.read_html(html)

            # Add df to list of dfs
            if dfs:
                df = dfs[0]
                # Convert the Date column to pandas datetime.date format (Only has year, month, day) for date comparisons in case most_recent_date was provided.
                df['Date'] = pd.to_datetime(df['Date']).dt.date
                if has_recent_date:
                    # Filter out rows with dates earlier than or equal to the most recent date
                    df = df[df['Date'] > most_recent_date]
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
         
        ### Update loop condition ###
        try:
            last_btn = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(EC.presence_of_element_located((By.ID, "datatable_last")))
            
            # Update loop condition for scraping all data (whether there's more data to scrape)
            last_btn_classes = last_btn.get_attribute("class")
            flag = "disabled" not in last_btn_classes
            
            # Update loop condition for scraping only new data
            if has_recent_date:
                if df.shape[0] < 10: # checking most recent data frame
                    # New data does not take up a full table of 10 rows, meaning there is no more new data. 
                    # Loop condition updated to false to stop scraping.
                    flag = False
                    print("No more new data, ending data scraping...")
        except Exception as e:
            print("Error whilst scraping table", table_num)
            print(f"An error has occurred whilst trying to locate last button with id datatable_last: {e}")
            break
        
        ### Click next button to go to next table ###
        if flag: # Only go to next page if loop condition is still true
            try:
                next_button = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(EC.element_to_be_clickable((By.ID, "datatable_next")))
                driver.execute_script("arguments[0].click();", next_button)
            except Exception as e:
                print("Error whilst scraping table", table_num)
                print(f"An error has occurred whilst trying to click next button with id datatable_next: {e}")
                break
        
        # DEBUG: Update table counter
        table_num += 1
        
        ### Wait for old element vars to go stale ###
        if flag:
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

def get_most_recent_date_from_db(table_name, db_connection):
    query = f"SELECT MAX(Date) FROM {table_name};"
    result = duckdb.query(query, connection=db_connection).fetchall()
    
    most_recent_date = result[0][0]
    
    # If the result is not a date object, convert it to pandas datetime.date format
    if not isinstance(most_recent_date, date):
        most_recent_date = pd.to_datetime(most_recent_date).date()
    
    return most_recent_date

def update_database(new_data_df, table_name, db_connection):
    if not new_data_df.empty:
        new_data_df.to_sql(table_name, db_connection, if_exists='append', index=False)
        print(f"Inserted {len(new_data_df)} new records into {table_name}.")
    else:
        print("No new data to insert.")

def clean_data(data):
    
    df = data.copy()
    # Drop first column
    df = df.drop(df.columns[0], axis=1)

    # Drop rows where critical fields have missing values
    critical_fields = ['Date', 'Brand Name(s)', 'Product Description', 'Product Type']
    df = df.dropna(subset=critical_fields)

    # Replace missing values in non-critical fields with "Not provided"
    df.fillna(value="Not provided", inplace=True)

    # Convert the Date column to pandas datetime.date format (Only has year, month, day)
    #df['Date'] = pd.to_datetime(df['Date']).dt.date

    # Ensure all other columns are of type string
    for col in df.columns:
        if col != 'Date':
            df[col] = df[col].astype(str)
    
    return(df)
 
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
    # TODO: Conditional for scraping all data or only new data after a given date
    data = scrape_data(driver, ignored_exceptions)
    
    # Exit driver at the end of the loop
    driver.quit()
    
    num_tables = len(data)
    print("Total tables scraped:", num_tables)
    
    data = pd.concat(data)
    print(data)
    
    # TODO: compare number of rows from pandas to the number of rows that the text for id="datatable_info" shows.
    
    ############################ TRANSFORM/CLEAN DATA ############################
    clean_data = clean_data(data)
    
    ############################ LOAD/SAVE DATA ############################
    # TODO: Load data into database (database TBD)
    
    
    
if __name__ == "__main__":
    main()