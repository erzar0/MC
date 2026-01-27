import os
import json
import time
import csv
import random
import signal
import re
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

ASSETS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "assets"))
INPUT_FILE = os.path.join(ASSETS_DIR, "pmc_data.csv")
RESULTS_FILE = os.path.join(ASSETS_DIR, "pmc_details_deep.csv")
STATE_FILE = os.path.join(ASSETS_DIR, "detail_crawl_state.json")
CHROME_PROFILE_PATH = os.path.join(os.getcwd(), "..", "tmp","pmc_profile")

class DetailCrawler:
    def __init__(self):
        self.running = True
        self.processed_ids = set()
        
        if not os.path.exists(ASSETS_DIR):
            os.makedirs(ASSETS_DIR)

        self.state = self.load_state()
        self.load_processed_ids()
        
        signal.signal(signal.SIGINT, self.handle_exit)

    def handle_exit(self, signum, frame):
        print("\n[!] Exit signal received. Saving state...")
        self.running = False

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except: pass
        return {"last_processed_index": 0}

    def save_state(self, index):
        with open(STATE_FILE, 'w') as f:
            json.dump({"last_processed_index": index}, f)

    def load_processed_ids(self):
        if os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.processed_ids.add(row['id'])
            print(f"Resuming: {len(self.processed_ids)} records already detailed.")

    def clean_one_line(self, text):
        if not text: return ""
        # Remove newlines, tabs, and multiple spaces
        text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        return re.sub(r'\s+', ' ', text).strip()

    def get_text(self, parent, selector, default=""):
        try:
            return parent.find_element(By.CSS_SELECTOR, selector).text.strip()
        except:
            return default

    def extract_deep_data(self, driver, project_id):
        # 1. Map Category (Breadcrumbs)
        # Targeted: The second link in the post_context div
        try:
            category_els = driver.find_elements(By.CSS_SELECTOR, ".post_context a")
            map_category = category_els[1].text.strip() if len(category_els) > 1 else "Unknown"
        except:
            map_category = "Unknown"

        # 2. Project Progress %
        # Extracts "100% complete" from the metadata table
        project_progress = "Unknown"
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "table.resource-info tr")
            for row in rows:
                if "Progress" in row.text:
                    project_progress = row.find_element(By.TAG_NAME, "td").text.strip()
                    break
        except: pass

        # 3. Platform & Author ID
        platform = self.get_text(driver, ".platform", "Unknown")
        author_id = "Unknown"
        try:
            author_id = driver.find_element(By.ID, "author_id").get_attribute("innerText").strip()
        except: pass

        # 4. Description (Formatted as one line)
        desc_raw = self.get_text(driver, "#r-text-block")
        description = self.clean_one_line(desc_raw)

        # 5. Image Gallery
        images = []
        img_elements = driver.find_elements(By.CSS_SELECTOR, "#light-gallery a.rsImg")
        for img in img_elements:
            href = img.get_attribute("href")
            if href: images.append(href)

        # 6. Metadata Dates (Published vs Updated)
        published_date = ""
        updated_date = ""
        try:
            date_block = driver.find_element(By.CSS_SELECTOR, ".post_date")
            dates = date_block.find_elements(By.TAG_NAME, "abbr")
            if len(dates) >= 1: updated_date = dates[0].get_attribute("title")
            if len(dates) >= 2: published_date = dates[1].get_attribute("title")
        except: pass

        # 7. Mirror URLs
        mirrors = []
        mirror_elements = driver.find_elements(By.CSS_SELECTOR, "ul.content-actions li a")
        for m in mirror_elements:
            m_url = m.get_attribute("href")
            m_name = self.clean_one_line(m.text)
            if m_url and ("download" in m_url or "mirror" in m_url):
                mirrors.append(f"{m_name} ({m_url})")

        return {
            "id": project_id,
            "author_id": author_id,
            "category": map_category,
            "platform": platform,
            "progress": project_progress,
            "description": description,
            "gallery_urls": " | ".join(images),
            "download_mirrors": " | ".join(mirrors),
            "date_published": published_date,
            "date_updated": updated_date,
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def run(self):
        if not os.path.exists(INPUT_FILE):
            print(f"Error: {INPUT_FILE} not found. Please run the main crawler first.")
            return

        # Load original CSV items
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            items = list(csv.DictReader(f))

        options = uc.ChromeOptions()
        options.add_argument(f"--user-data-dir={CHROME_PROFILE_PATH}")
        driver = uc.Chrome(options=options)
        
        start_idx = self.state["last_processed_index"]

        try:
            for i in range(start_idx, len(items)):
                if not self.running: break
                
                project = items[i]
                p_id = project['id']
                url = project['url']

                if p_id in self.processed_ids:
                    continue

                print(f"[{i+1}/{len(items)}] Scraping Details: {project['title']}...")
                
                try:
                    driver.get(url)
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "resource_object")))
                    time.sleep(random.uniform(3.5, 6.0)) # Polite delay

                    deep_data = self.extract_deep_data(driver, p_id)
                    
                    # Merge with some original data for reference if needed, 
                    # but here we just write the detail row
                    file_exists = os.path.isfile(RESULTS_FILE)
                    with open(RESULTS_FILE, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=deep_data.keys())
                        if not file_exists: writer.writeheader()
                        writer.writerow(deep_data)

                    self.processed_ids.add(p_id)
                    self.save_state(i)

                except Exception as e:
                    print(f"Error on {url}: {e}")
                    time.sleep(5)
                    continue

        finally:
            driver.quit()
            print(f"\n[Done] Deep details saved to: {RESULTS_FILE}")

if __name__ == "__main__":
    crawler = DetailCrawler()
    crawler.run()