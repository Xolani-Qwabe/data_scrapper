import time
import os
import re
import pandas as pd
from io import StringIO
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# Setup Chrome driver
def get_driver() -> webdriver:
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(executable_path="chromedriver.exe")
    return webdriver.Chrome(service=service, options=chrome_options)

driver = get_driver()

def safe_get(url):
    try:
        driver.get(url)
        time.sleep(6)
    except Exception as e:
        print(f"⚠ Retry due to: {e}")
        time.sleep(6)
        driver.get(url)

# Get all box-score links from schedule
def get_box_score_links(url: str):
    safe_get(url)
    links = driver.find_elements(By.PARTIAL_LINK_TEXT, "Box Score")
    return [link.get_attribute("href") for link in links]

# Extract all tables from page
def extract_all_tables():
    return re.findall(
        r"(<table[^>]*?id=\"([^\"]+)\"[\s\S]*?</table>)",
        driver.page_source
    )

# HTML table → CSV
def html_table_to_df(html, table_name):
    df = pd.read_html(StringIO(html))[0]

    os.makedirs("./nba_data", exist_ok=True)
    current_date = datetime.now().strftime("%Y-%m-%d")
    filepath = f"./nba_data/{table_name}_{current_date}.csv"

    df.to_csv(filepath, index=False)
    print(f"🏀 Saved: {filepath}")

# All schedule pages
urls = [
    "https://www.basketball-reference.com/leagues/NBA_2026_games.html",
    "https://www.basketball-reference.com/leagues/NBA_2026_games-november.html",
    "https://www.basketball-reference.com/leagues/NBA_2026_games-december.html",
]

print("🚀 Collecting box score links...")
all_box_scores = []

for u in urls:
    all_box_scores.extend(get_box_score_links(u))

print(f"🔗 Found {len(all_box_scores)} box score links\n")

# Scrape every game
for i, link in enumerate(all_box_scores):
    print(f"Scraping ({i+1}/{len(all_box_scores)}): {link}")
    safe_get(link)

    game_id = link.split("/")[-1].replace(".html", "")

    tables = extract_all_tables()
    if not tables:
        print("⚠ No tables found\n")
        continue

    for html, table_id in tables:
        if table_id.startswith("box-"):
            html_table_to_df(html, f"{game_id}_{table_id}")

print("\n✅ Done scraping!")
input("Press ENTER to close browser...")
driver.quit()
