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
import duckdb_engine
from datetime import date
import re
from sqlalchemy import create_engine

############################ FUNCTIONS ############################
def scrape_data(driver, ignored_exceptions, most_recent_date=None):
    """
    Scrapes data from HTML tables on the FDA website using Selenium WebDriver and returns a list of pandas DataFrames.
    If a `most_recent_date` is provided, the function filters out any rows with dates earlier than or equal to this date.
    The function continues to scrape until all new data is collected or there are no more pages to scrape.

    Args:
        driver (selenium.webdriver): The Selenium WebDriver instance used to interact with the webpage.
        ignored_exceptions (tuple): A tuple of exceptions that should be ignored during the WebDriverWait operations.
        most_recent_date (datetime.date, optional): The most recent date found in the database. If provided, only data more 
        recent than this date will be scraped. Defaults to None.

    Returns:
        list of pd.DataFrame: A list of pandas DataFrames, each containing the data from one page of the table. The DataFrames 
        are filtered to exclude any rows with dates earlier than or equal to `most_recent_date`, if provided.
    """
    
    # List of pandas dfs to be returned
    dataframes = []
    
    # Loop condition variables
    flag = True
    has_recent_date = most_recent_date is not None
    
    # DEBUG: Tables scraped counter
    table_num = 1
    
    # Load HTML to scrape data
    html = StringIO(driver.page_source)
    
    # Data Scraping loop
    while(flag):        
        ### Extract the HTML table ###
        try:
            # Load HTML to scrape data
            html = StringIO(driver.page_source)

            # Read HTML with pandas
            dfs = pd.read_html(html)

            # Add first and only df from scraped list of df to final list of dfs
            if dfs:
                df = dfs[0]
                # Convert the Date column to pandas datetime.date format (Only has year, month, day) for date comparisons in case most_recent_date was provided
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
            # Update loop condition for scraping all data (whether there's more data to scrape)
            last_btn = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions).until(EC.presence_of_element_located((By.ID, "datatable_last")))
            last_btn_classes = last_btn.get_attribute("class")
            flag = "disabled" not in last_btn_classes
            
            # Update loop condition for scraping only new data
            if has_recent_date:
                if df.shape[0] < 10: # checking most recent data frame
                    # New data does not take up a full table of 10 rows, meaning there are no more new data
                    # Loop condition updated to false to stop scraping
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
        
        ### Wait for old element vars to go stale before moving onto next page to avoid exceptions ###
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
    """
    Retrieves the most recent date from the specified table in the DuckDB database.
    If the table does not exist or the query returns no result, it returns None.

    Args:
        table_name (str): The name of the table from which to retrieve the most recent date.
        db_connection (duckdb.DuckDBPyConnection): A connection object to the DuckDB database.

    Returns:
        datetime.date or None: The most recent date found in the table, or None if no date is found.
    """
    
    try:
        query = f"SELECT MAX(Date) FROM {table_name};"
        result = duckdb.query(query, connection=db_connection).fetchall()
        
        most_recent_date = result[0][0] if result and result[0][0] is not None else None
        
        # If most_recent_date is not None and not a date object, convert it to pandas datetime.date format
        if most_recent_date is not None and not isinstance(most_recent_date, date):
            most_recent_date = pd.to_datetime(most_recent_date).date()
        
        return most_recent_date
    
    except duckdb.Error as e:
        # Database does not exist
        print(f"An error occurred: {e}") 
        return None

def clean_data(data):
    """
    Cleans the extracted FDA food recall data by handling missing values, ensuring proper data types, and standardizing field names.

    Args:
        data (pd.DataFrame): The DataFrame containing the raw extracted data.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    
    df = data.copy()

    # Drop rows where critical fields have missing values
    critical_fields = ['Brand Name(s)', 'Product Description', 'Product Type']
    df = df.dropna(subset=critical_fields)

    # Replace missing values in non-critical fields with "Not provided"
    df.fillna(value="Not provided", inplace=True)

    # Ensure all other columns are of type string
    for col in df.columns:
        if col != 'Date':
            df[col] = df[col].astype(str)

    # Format columns to uppercase with underscores instead of spaces and remove parentheses 
    df.columns = [re.sub(r'[()]+', '', col).replace(' ', '_').upper() for col in df.columns]
    
    return df

def generate_create_table_statement(df, table_name):
    """
    Generates a SQL CREATE TABLE statement based on a given pandas DataFrame. 
    Maps pandas data types to corresponding SQL data types used in DuckDB.
    
    Args:
        df (pd.DataFrame): The DataFrame from which to generate the SQL table schema.
        table_name (str): The name of the table to be created.

    Returns:
        str: A SQL CREATE TABLE statement.
    """
    
    # Mapping pandas dtypes to DuckDB SQL types
    dtype_mapping = {
        'int64': 'BIGINT',
        'float64': 'DOUBLE',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'DATE',
        'object': 'TEXT'
    }
    
    create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    
    # Loops through each column and its dtype to add column definitions 
    column_definitions = []
    for column in df.columns:
        col_name = column.replace(" ", "_").upper() 
        col_dtype = str(df[column].dtype)
        # Default to TEXT if dtype not found
        sql_type = dtype_mapping.get(col_dtype, 'TEXT')  
        
        column_definitions.append(f"{col_name} {sql_type}")
    
    # Combine column definitions
    create_stmt += ", ".join(column_definitions) + ");"
    
    return create_stmt

def initialize_database(db_connection, df, table_name):
    """
    Initializes a table in the DuckDB database based on a pandas DataFrame schema.
    Generates a SQL CREATE TABLE statement from the DataFrame's columns and data types,
    then executes the statement to create the table in the DuckDB database if it doesn't already exist.

    Args:
        db_connection (duckdb.DuckDBPyConnection): A connection object to the DuckDB database.
        df (pd.DataFrame): The DataFrame used to define the table schema.
        table_name (str): The name of the table to be created.

    Returns:
        None
    """
    
    create_table_query = generate_create_table_statement(df, table_name)
    
    # Debug: Print the generated CREATE TABLE statement
    print("Generated SQL statement:", create_table_query)
    
    # Execute the CREATE TABLE statement
    db_connection.execute(create_table_query) 

def update_database(df, table_name, engine):
    """
    Updates the specified table in the database with new data from a pandas DataFrame.
    Checks if the DataFrame is not empty and then inserts its rows into the specified table
    in the database. If the DataFrame is empty, no data is inserted.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be inserted.
        table_name (str): The name of the table into which the data will be inserted.
        engine (sqlalchemy.engine.Engine): The database engine object used to execute the SQL statement.

    Returns:
        None
    """
    
    if not df.empty:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Inserted {len(df)} new rows into {table_name}.")
    else:
        print("No new data to insert.")
        
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
    
    # Initialize database connection
    db_name = 'fda_recall_etl.db'
    db_connection = duckdb.connect(db_name) 
    table_name = 'recalled_products'
    
    # Initialize web driver
    driver = webdriver.Chrome(options=options)
    driver.get(URL)
    
    ############################ SCRAPE DATA ############################
    # will fetch most_recent_date if db exists
    most_recent_date = get_most_recent_date_from_db(table_name, db_connection) 
    
    if most_recent_date is None:
        # Scrape all data
        data = scrape_data(driver, ignored_exceptions)
    else:
        # Scrape data after most recent date
        data = scrape_data(driver, ignored_exceptions,most_recent_date=most_recent_date)
    
    # Exit driver
    driver.quit()
    
    num_tables = len(data)
    print("Total tables scraped:", num_tables)
    data = pd.concat(data)
    print(data)
        
    ############################ TRANSFORM/CLEAN DATA ############################ 
    cleaned_data = clean_data(data)
    print("Clean data:")
    print(cleaned_data)
    
    ############################ LOAD/SAVE DATA ############################
    initialize_database(db_connection, cleaned_data, table_name)
    # Create a SQLAlchemy engine for DuckDB
    engine = create_engine('duckdb:///' + db_name)
    update_database(cleaned_data, table_name, engine)
    
    db_connection.close()
     
if __name__ == "__main__":  
    main() 

