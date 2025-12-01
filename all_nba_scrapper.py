import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
from io import StringIO
import re
from datetime import datetime
import os

# Create folder
SAVE_FOLDER = "./nba_data"
os.makedirs(SAVE_FOLDER, exist_ok=True)

# Set up driver
def get_driver():
    service = Service(executable_path="chromedriver.exe")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    return webdriver.Chrome(service=service, options=options)

driver = get_driver()

# Extract HTML table even if it's commented out
def get_table_by_id(table_id):
    source = driver.page_source

    # Try normal
    try:
        el = driver.find_element(By.ID, table_id)
        return el.get_attribute("outerHTML")
    except:
        pass

    # Try commented HTML
    comment_pattern = rf"<!--([\s\S]*?<table[^>]*id=\"{table_id}\"[\s\S]*?</table>)-->"
    m = re.search(comment_pattern, source)
    return m.group(1) if m else None

def save_df(df, game_id, name):
    current_date = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(SAVE_FOLDER, f"{game_id}_{name}_{current_date}.csv")
    df.to_csv(path, index=False)
    print(f"📁 Saved: {path}")

def scrape_box_score(game_id, url):
    driver.get(url)
    time.sleep(3)

    tables = re.findall(r"(<table[^>]*id=\"([^\"]+)\".*?</table>)", driver.page_source)
    dfs = {}

    for html, tid in tables:
        if "box" in tid and "game" not in tid:  # exclude team summary table
            df = pd.read_html(StringIO(html))[0]
            dfs[tid] = df
            save_df(df, game_id, tid)

    return dfs

def scrape_pbp(game_id):
    pbp_url = f"https://www.basketball-reference.com/boxscores/pbp/{game_id}.html"
    driver.get(pbp_url)
    time.sleep(3)

    html = get_table_by_id("pbp")
    if html:
        df = pd.read_html(StringIO(html))[0]
        save_df(df, game_id, "pbp")
        return df
    return None

def scrape_shots(game_id):
    shot_url = f"https://www.basketball-reference.com/boxscores/shot-chart/{game_id}.html"
    driver.get(shot_url)
    time.sleep(3)

    html = get_table_by_id("shots")
    if html:
        df = pd.read_html(StringIO(html))[0]
        save_df(df, game_id, "shots")
        return df
    return None

# Merge all tables to one dataset
def combine_stats(game_id, dfs, pbp, shots):
    combined = []

    for k, df in dfs.items():
        df["table"] = k
        combined.append(df)

    if pbp is not None:
        pbp["table"] = "pbp"
        combined.append(pbp)

    if shots is not None:
        shots["table"] = "shots"
        combined.append(shots)

    if combined:
        merged = pd.concat(combined, ignore_index=True)
        save_df(merged, game_id, "combined")

# ---- MAIN -----

schedule_urls = [
    "https://www.basketball-reference.com/leagues/NBA_2026_games.html",
    "https://www.basketball-reference.com/leagues/NBA_2026_games-november.html",
]

box_links = []
for url in schedule_urls:
    driver.get(url)
    time.sleep(3)
    links = driver.find_elements(By.PARTIAL_LINK_TEXT, "Box Score")
    box_links.extend(l.get_attribute("href") for l in links)

print(f"🔗 Found {len(box_links)} games")

for link in box_links[:5]:  # TEST on first 5 games
    print("📌 Scraping:", link)
    game_id = link.split("/")[-1].replace(".html", "")

    box = scrape_box_score(game_id, link)
    pbp = scrape_pbp(game_id)
    shots = scrape_shots(game_id)

    combine_stats(game_id, box, pbp, shots)

driver.quit()
print("🎯 DONE — All data collected!")
