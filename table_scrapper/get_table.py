import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
from io import StringIO
import re
from datetime import datetime

# Set up Chrome driver



def get_driver() -> webdriver:
    service = Service(executable_path="chromedriver.exe")
    return webdriver.Chrome(service=service)


driver = get_driver()

# get page from url given 
def get_html_page(url : str):
    page = driver.get(url)
    time.sleep(10)
    return page

def get_table_by_id(element_id: str):
    el = driver.find_element(By.ID, element_id )

    # Get HTML content of the element
    html = el.get_attribute("outerHTML")

    # Handle FBref’s commented table case
    if "<table" not in html:
        print("Table might be inside an HTML comment, extracting from page source...")
        page_source = driver.page_source

        match = re.search(
            r"<!--([\s\S]*?<table[^>]*id=\"results2025-202691_overall\"[\s\S]*?</table>)-->",
            page_source
        )
        if match:
            html = match.group(1)
        else:
            print("Could not find table inside HTML comments.")
            driver.quit()
            exit()
    return html

def html_table_to_df(html, table_name):
    # Parse table HTML into a DataFrame (using StringIO to avoid warnings)
    df = pd.read_html(StringIO(html))[0]
    current_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"{table_name}_{current_date}.csv"
    # Check and save DataFrame to CSV
    if not df.empty:
        df.to_csv(f'./data/{filename}', index=False)
        print(f"Table successfully written to '{filename}'")
    else:
        print("DataFrame is empty — no data extracted.")
    return df