import os
import json
import time
import csv
import random
import signal
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

ASSETS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "assets"))
STATE_FILE = os.path.join(ASSETS_DIR, "crawl_state.json")
RESULTS_FILE = os.path.join(ASSETS_DIR, "pmc_data.csv")
CHROME_PROFILE_PATH = os.path.join(os.getcwd(), "..", "tmp","pmc_profile")


YEARS_TO_SCRAPE = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]

class Crawler:
    def __init__(self):
        self.running = True
        self.processed_urls = set()
        
        if not os.path.exists(ASSETS_DIR):
            os.makedirs(ASSETS_DIR)
            print(f"Created directory: {ASSETS_DIR}")
            
        self.state = self.load_initial_state()
        self.load_processed_from_csv()
        
        signal.signal(signal.SIGINT, self.handle_exit)

    def handle_exit(self, signum, frame):
        print("\n[!] Exit signal received. Saving progress and closing browser...")
        self.running = False

    def load_initial_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except: pass
        return {"year_idx": 0, "page": 1}

    def load_processed_from_csv(self):
        if os.path.exists(RESULTS_FILE):
            print(f"Loading previous results from {RESULTS_FILE}...")
            with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.processed_urls.add(row['url'])
            print(f"Deduped {len(self.processed_urls)} records.")

    def save_progress(self, y_idx, p_num):
        with open(STATE_FILE, 'w') as f:
            json.dump({"year_idx": y_idx, "page": p_num}, f)

    def get_safe_text(self, parent, selector, attr=None):
        try:
            el = parent.find_element(By.CSS_SELECTOR, selector)
            return el.get_attribute(attr) if attr else el.text.strip()
        except:
            return "0" if "num" in selector or "span" in selector else ""

    def run(self):
        y_idx = self.state["year_idx"]
        p_num = self.state["page"]

        options = uc.ChromeOptions()
        options.add_argument(f"--user-data-dir={CHROME_PROFILE_PATH}")
        driver = uc.Chrome(options=options)
        
        try:
            while y_idx < len(YEARS_TO_SCRAPE) and self.running:
                year = YEARS_TO_SCRAPE[y_idx]
                url = f"https://www.planetminecraft.com/projects/?mode=advanced&share%5B%5D=world_link&platform=1&monetization%5B%5D=0&monetization%5B%5D=1&time_machine=y-{year}&order=order_downloads&p={p_num}"
                
                print(f"--- [Year: {year}] [Page: {p_num}] [Unique: {len(self.processed_urls)}] ---")
                driver.get(url)
                
                try:
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    time.sleep(random.uniform(2.5, 4.0)) 
                    
                    items = driver.find_elements(By.CSS_SELECTOR, "li.resource[data-type='resource']")
                    
                    if not items:
                        print(f"Year {year} appears exhausted.")
                        y_idx += 1
                        p_num = 1
                        self.save_progress(y_idx, p_num)
                        continue

                    for item in items:
                        if not self.running: break
                        
                        try:
                            title_el = item.find_element(By.CSS_SELECTOR, "a.r-title")
                            href = title_el.get_attribute("href")
                            
                            if href in self.processed_urls: continue

                            data = {
                                "id": item.get_attribute("data-id"),
                                "year_filter": year,
                                "title": title_el.text.strip(),
                                "url": href,
                                "category": self.get_safe_text(item, ".r-subject"),
                                "creator": self.get_safe_text(item, ".activity_name"),
                                "creator_id": self.get_safe_text(item, ".activity_name", "data-mid"),
                                "views": self.get_safe_text(item, "i.visibility + span"),
                                "downloads": self.get_safe_text(item, "i.get_app + span"),
                                "comments": self.get_safe_text(item, "i.chat_bubble + span"),
                                "diamonds": self.get_safe_text(item, ".c-num-votes"),
                                "favorites": self.get_safe_text(item, ".c-num-favs"),
                                "published_date": self.get_safe_text(item, ".contributed abbr.timeago", "title"),
                                "scraped_timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                            }

                            file_exists = os.path.isfile(RESULTS_FILE)
                            with open(RESULTS_FILE, 'a', newline='', encoding='utf-8') as f:
                                writer = csv.DictWriter(f, fieldnames=data.keys())
                                if not file_exists: writer.writeheader()
                                writer.writerow(data)
                            
                            self.processed_urls.add(href)
                        except: continue

                    try:
                        driver.find_element(By.CSS_SELECTOR, "a.pagination_next")
                        p_num += 1
                        self.save_progress(y_idx, p_num)
                        time.sleep(random.uniform(4.0, 7.0))
                    except:
                        print(f"End of Year {year}. Advancing...")
                        y_idx += 1
                        p_num = 1
                        self.save_progress(y_idx, p_num)

                except Exception as e:
                    if not self.running: break
                    print(f"Network or Page Error: {e}. Sleeping 10s...")
                    time.sleep(10)

        finally:
            if driver: driver.quit()
            print(f"\n[Done] Assets updated in: {ASSETS_DIR}")

if __name__ == "__main__":
    crawler = Crawler()
    crawler.run()
